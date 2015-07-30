var collector = require('node-netflowv9');
var loki = require('lokijs');
var db = new loki('db.json');

// create collection with indices
var hosts = db.addCollection('hosts',{indices:['i','ip']});

var ipIndex = {};

collector(function(flow) {

	// get current timestamp
	var ts = Math.round((new Date()).getTime() / 1000);

	// regex for finding our addresses
	var re = new RegExp("(^127\.0\.0\.1)|(^10\.)|(^172\.1[6-9]\.)|(^172\.2[0-9]\.)|(^172\.3[0-1]\.)|(^192\.168\.)");

	for (i in flow.flows) {

		// if doClear is set, clear first
		if (doClear > 0) {

			console.log('clearing db');

			// first remove from the db if under threshold of doClear
			hosts.removeWhere({'i':{'$lt':doClear}});

			// then loop through all the db entries to clean up the ipIndex
			var e = hosts.find();

			// reset ipIndex
			ipIndex = [];

			// now put db values back in it
			for (var c=0; c<e.length; c++) {
				ipIndex[e[c].h] = e[c]['$loki'];
			}

			// finally set doClear back to 0
			doClear = 0;
		}

		var f = flow.flows[i];

		// get important bits of data
		var srcIp = f.ipv4_src_addr;
		var dstIp = f.ipv4_dst_addr;
		var b = f.in_bytes;

		// find our addresses
		var dir = -1;
		var ip = '';
		if (re.test(srcIp)) {
			dir = 1;
			ip = srcIp;
		} else if (re.test(dstIp)) {
			dir = 0;
			ip = dstIp;
		}

		if (dir != -1) {

			// find host in ipIndex
			if (typeof(ipIndex[ip]) == 'undefined') {

				// storage for the $loki id
				var l = 0;

				// this is a new host, insert it
				if (dir == 1) {
					l = hosts.insert({h:ip,i:0,o:b,firstTs:ts})['$loki'];
				} else {
					l = hosts.insert({h:ip,i:b,o:0,firstTs:ts})['$loki'];
				}

				// add the $loki id to the ipIndex
				ipIndex[ip] = l;

			} else {

				// this is an existing host, update data for counters

				// find it with get, faster binary search
				var o = hosts.get(ipIndex[ip]);

				if (dir == 1) {
					// users upload or out from user perspective
					o.o += b;
				} else {
					// users download or in from user perspective
					o.i += b;
				}
				hosts.update(o);

			}

		}

	}

	console.log('--------');

	// header
	console.log("\x1b[32m",'Host',"\033[19G\x1b[30m",'Download',"\033[49G",'Upload');

	// get data sorted by i with a limit
	var d = hosts.chain().find().simplesort('i',true).limit(25).data();

	for (var i=0; i<d.length; i++) {
		console.log("\x1b[32m",d[i].h,"\033[20G\x1b[30m"+bytesToSize(d[i].i)+'\033[32G'+getBps(d[i].i,d[i].firstTs,ts),"\033[50G"+bytesToSize(d[i].o)+'\033[62G'+getBps(d[i].o,d[i].firstTs,ts));
	}


}).listen(3000);

// every 5 minutes clear out devices under threshold of N MB downloaded
var doClear = 0;
var clearInterval = setInterval(function() {
	doClear = 1000000*3;
}, 1000*60*5);

function getBps(bytes,startTs,ts) {
	// first multiply bytes by 8 to get bits
	var bits = bytes*8;

	// then divide bits by number of seconds open
	var bps = bits/(ts-startTs);

	return bitsToSize(bps);
}

function bitsToSize(bits) {
    var i = Math.floor( Math.log(bits) / Math.log(1000) );
    return ( bits / Math.pow(1000, i) ).toFixed(2) * 1 + ' ' + ['bps', 'kbps', 'mbps', 'gbps', 'tbps'][i];
};

function bytesToSize(bytes) {
    var i = Math.floor( Math.log(bytes) / Math.log(1024) );
    return ( bytes / Math.pow(1024, i) ).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
};
