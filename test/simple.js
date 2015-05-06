var mmdbreader = require('../index');
var ipdata = [
	['8.8.8.8','US'],
	['2600:1010:b00e:4658:488b:55d4:86a4:3c61','US'],
	['141.136.120.166','NL']
];

var passed = "\033[0;32mPassed\033[0m";
var failed = "\033[0;31mFailed\033[0m";
var check = function(geo, data){
	if(geo && geo.country && geo.country.iso_code){
		if(geo.country.iso_code == data[1]){
			console.log(' '+passed+' '+data[0]+' = '+data[1]);
		}else{
			console.log(' '+failed+' wrong data for '+data[0]+' ('+data[1]+' != ' + geo.country.iso_code + ')');
		}
	}else{
		console.log(' '+failed+' couldn\'t read data for '+data[0]+' ('+data[1]+')');
	}
}
// SYNC
console.log('SYNC');
// open database
var countries = mmdbreader.openSync('./GeoLite2-City.mmdb');
ipdata.forEach(function(data,i){
	// read data
	var geo = countries.getGeoDataSync(data[0]);
	check(geo, data);
});
// ASYNC
console.log('ASYNC');
// open database
mmdbreader.open('./GeoLite2-City.mmdb',function(err,countries){
	if(err){
		console.log(' '+failed+' Can\'t open database: ' + err.message);
		return;
	}
	var i=0,
		get = function(){
			if(i < ipdata.length){
				var data = ipdata[i++];
				countries.getGeoData(data[0],function(err,geo){
					if(err){
						console.log(' '+failed+' Can\'t read for '+data[0]+' ('+data[1]+'): ' + err.message);
					}else{
						check(geo,data);
						get();
					}
				});
			} else {
				doSpeedtest();
			}
		};
	get();
});

function doSpeedtest() {
	console.log("Cache speed test");
	// open database

	var randomIps = require('./mass.json');

	console.time('Cached test w/ path');
	var database = mmdbreader.openSync('./GeoLite2-City.mmdb');
	for (var i = 0; i < randomIps.length; i++) {
		database.getGeoDataSync(randomIps[i], ['country', 'names', 'en']);
	}
	console.timeEnd('Cached test w/ path');

	console.time('Not cached test w/ path');
	database = mmdbreader.openSync('./GeoLite2-City.mmdb');
	for (var i = 0; i < randomIps.length; i++) {
		database.getGeoDataSync(randomIps[i], ['country', 'names', 'en']);
		database.reader._cachedIpv4StartNode = false;
	}
	console.timeEnd('Not cached test w/ path');

	console.time('Cached test');
	var database = mmdbreader.openSync('./GeoLite2-City.mmdb');
	for (var i = 0; i < randomIps.length; i++) {
		database.getGeoDataSync(randomIps[i]);
	}
	console.timeEnd('Cached test');

	console.time('Not cached test');
	database = mmdbreader.openSync('./GeoLite2-City.mmdb');
	for (var i = 0; i < randomIps.length; i++) {
		database.getGeoDataSync(randomIps[i]);
		database.reader._cachedIpv4StartNode = false;
	}
	console.timeEnd('Not cached test');

}