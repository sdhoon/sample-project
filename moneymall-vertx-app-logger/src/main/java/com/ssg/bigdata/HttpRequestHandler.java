package com.ssg.bigdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.util.UriEncoder;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;

@Component
public class HttpRequestHandler implements Handler<HttpServerRequest>{

	@Override
	public void handle(HttpServerRequest request) {
		String path = request.path();
		
		if(path.contains("app")) {	
			request.bodyHandler(new AppHandler());
			//if (request.getParam("data")!=null) System.out.println("TEST DATA == " + request.getParam("data").toString());
			request.response().setStatusCode(200);
			request.response().setStatusMessage("OK");
		} else if(path.contains("check1")) {	
			request.bodyHandler(new Check1Handler());
			request.response().setStatusCode(200);
			request.response().setStatusMessage("OK");
		} else if(path.contains("check2")) {	
			request.bodyHandler(new Check2Handler());
			request.response().setStatusCode(200);
			request.response().setStatusMessage("OK");
		} else {
			request.response().setStatusCode(404);
			request.response().setStatusMessage("NOT ALLOWED");
		}
		
		request.response().headers()
			.add("Access-Control-Allow-Origin", "*")
			.add("content-type", "text/plain")
			.add("Connection","close");	//20141219 남정달과장님 요청으로 추가
		request.response().end();
	}

}

class AppHandler implements Handler<Buffer>
{		
	private static Logger log = LoggerFactory.getLogger("app-error-log");
	
	//@Autowired
	//private KafkaDispatcher kafkaDispatcher;

	@Override
	public void handle(Buffer body) {	
		
		String data;
		try {
			data = new String(body.getBytes(),"UTF-8");
			//System.out.println("data == " + data);
			
			if (data!=null && !data.equals("") && data.length() > 0) log.info(UriEncoder.decode(data.substring(5)));
			//if (data!=null) log.info(data);
			
			//아직 사용안함
			//kafkaDispatcher.dispatch(data);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		 
	}		  
}

class Check1Handler implements Handler<Buffer>
{		
	private static Logger log = LoggerFactory.getLogger("app-check1-log");

	@Override
	public void handle(Buffer body) {	
		
		String data;
		try {
			data = new String(body.getBytes(),"UTF-8");
			if (data!=null && !data.equals("") && data.length() > 0) log.info(UriEncoder.decode(data.substring(5)));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		 
	}		  
}

class Check2Handler implements Handler<Buffer>
{		
	private static Logger log = LoggerFactory.getLogger("app-check2-log");

	@Override
	public void handle(Buffer body) {	
		
		String data;
		try {
			data = new String(body.getBytes(),"UTF-8");
			if (data!=null && !data.equals("") && data.length() > 0) log.info(UriEncoder.decode(data.substring(5)));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		 
	}		  
}
