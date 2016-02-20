/*
 * META API
 *
 * @author META Platform <www.meta-platform.com>
 * @license See LICENSE file distributed with this source code
 */

var logger = require("meta-logger").facility("Monolit");

var Api = require("meta-api-local");

/*
 * Monolitic service manager with local broker
 *
 * Creates service manager
 *
 * @param Object options
 *		broker: { ... meta-api-local/Broker options ... },
 * 		server: { ... meta-api-local/Server options ... },
 */
var Monolit = function(options){

	if(!options) options = {};

	logger.info("Initializing Monolit...");

	var brokerAuth = new Api.BrokerLocalAuth(options.brokerAuthDb || "./broker.auth.json");

	if(!options.broker) options.broker = {};
	options.broker.authProvider = brokerAuth;

	this.broker = new Api.Broker(Api.MemoryQueue, options.broker || {});
	this.server = new Api.Server(this.broker, options.server || {});

	this.services = [];

	logger.info("Starting API server...");

};

/*
 * Loads service module
 *
 * @param String serviceName
 * @param Function serviceModule Init function for service (api, opts) -> ...
 */
Monolit.prototype.service = function(serviceName, serviceModule, config, opts){

	if(!opts) opts = {};

	this.services.push({
		name: serviceName,
		module: serviceModule,
		config: config || {},
		critical: ( opts.critical !== undefined ? opts.critical : true ),
		wait: ( opts.wait !== undefined ? opts.wait : true )
	});

};

Monolit.prototype.loadService = function(service){

	var self = this;

	return new Promise(function(resolve, reject){

		try {

			//TO-DO: Load config from global registry service?
			var opts = service.config;
			opts.serviceName = service.name;

			//Create API client and connect
			var apiClient = new Api.Client(service.name);

			apiClient.connect(self.broker, opts.secret).then(function(){

				//Load module
				try {

					var ret = service.module.call(null, apiClient, opts);

					if(ret instanceof Promise)
						ret.then(resolve, reject);
					else
						resolve();

				} catch(e){
					reject(e);
				}

			}, reject);

		} catch(e){
			reject(e);
		}

	});

};

Monolit.prototype.start = function(){

	var self = this;
	var task = Promise.resolve();

	//Load services
	var addService = function(service){

		return function(){

			logger.info("Loading service {%s} ...", service.name);

			return new Promise(function(resolve, reject){

				var resolved = false;

				//Wait?
				if(!service.wait && !service.critical){
					resolved = true;
					resolve();
				}

				self.loadService(service).then(function(){

					logger.info("Service {%s} loaded.", service.name);

					if(!resolved) resolve();

				}, function(err){

					logger.error("Failed to load service {%s}:", service.name, err, err.stack);
					
					if(!resolved && service.critical) reject(err);
					if(!resolved && !service.critical) resolve();

				});

			});

		};

	};

	for(var i in this.services)
		task = task.then(addService(this.services[i]));

	//Start API server
	task.then(function(){

		return self.server.start().then(function(){
			logger.info("API server started.");
		}, function(err){
			logger.error("Failed to start API server:", err);
		});

	});

	return task;

};

module.exports = Monolit;