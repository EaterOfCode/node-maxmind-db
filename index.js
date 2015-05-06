'use strict';

var Reader = require('./lib/Reader.js'),
    MaxmindDBReader;

MaxmindDBReader = module.exports = function () {
	// allow creation without 'new' keyword
	if (!(this instanceof MaxmindDBReader))
		return new MaxmindDBReader();
};

MaxmindDBReader.open = function(database,callback){
    Reader.open(database,function(err, reader){
        if(err){
            return callback(err);
        }
        var mmdbreader = MaxmindDBReader();
        mmdbreader.reader = reader;
        callback(null,mmdbreader);
    });
}

MaxmindDBReader.openSync = function(database){
    var mmdbreader = MaxmindDBReader();
    mmdbreader.reader = Reader.openSync(database);
    return mmdbreader;
}

MaxmindDBReader.prototype.getGeoData = function getGeoData(ipAddress, path, callback) {
    this.reader.get(ipAddress, path, callback);
};

MaxmindDBReader.prototype.getGeoDataSync = function getGeoDataSync(ipAddress, path) {
    return this.reader.getSync(ipAddress, path);
};

MaxmindDBReader.prototype.getDatabaseMetadata = function getDatabaseMetadata() {
    return this.reader.getMetadata();
};
