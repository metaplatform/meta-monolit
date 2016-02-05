var logger = require("meta-logger");
var Monolit = require("../index.js");

//Setup targets 
logger.toConsole({
	level: "info",
	timestamp: true,
	colorize: true
});

var myModule = function(api, opts){

	logger.debug("MyModule INIT", opts);

	api.subscribeQueue("test", function(msg){

		console.log("MyModule MSG", msg);

		return Promise.resolve(false);

	}).then(function(){

		logger.debug("MyModule subscribed.");

	});

	return new Promise(function(resolve, reject){

		setTimeout(function(){

			if(opts.fail)
				reject(new Error("FAILED!"));
			else
				resolve();

		}, opts.delay);

	});

};

var app = new Monolit();

app.service("myModule1", myModule, { fail: false, delay: 100 });
app.service("myModule2", myModule, { fail: false, delay: 100 });

app.start().then(function(){
	
	logger.debug("Started");

}, function(err){

	logger.error("Failed to start app.", err);
	process.exit();

});