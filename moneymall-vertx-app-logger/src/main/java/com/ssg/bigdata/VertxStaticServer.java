package com.ssg.bigdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;

@Component
public class VertxStaticServer extends AbstractVerticle {
	
	@Autowired
	AppConfiguration configuration;
	
	@Autowired
	Handler<HttpServerRequest> httpRequestHandler;	
	
	@Override
	public void start() throws Exception {
		
	    HttpServerOptions httpServerOptions = new HttpServerOptions();
	    httpServerOptions.setCompressionSupported(true);

	    vertx.createHttpServer(httpServerOptions)
	    	.requestHandler(this.httpRequestHandler)
	    	.listen(configuration.httpPort());
	}
}
