package com.ssg;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ssg.cassandra.Connection;
import com.ssg.clip.ClipVerticle;
import com.ssg.configuration.Configuration;
import com.ssg.dto.RecommendResponse;
import com.ssg.order.OrderVerticle;
import com.ssg.recommendation.RecommendVerticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;


/*  FOR Eclipse execution
 * 
 *  Run Configurations -> Java Application -> R Click -> New
 *	Main Tab
 *	Main class : io.vertx.core.Starter
 * 
 *  program arguments
 *  run com.ssg.VertxHttpServerVerticle
 *  
 *   VMarguments
 *  -Dcontainer.name=dev_vertx1 -Dlogback.configuration=logback.xml -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory
 *  
 *  
 *  
 *  FOR PROD
 *  java -Dcontainer.name=person1 -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory 
 *  -jar /data01/webroot/ssg-personalisation.vertx-webapp-0.0.1-SNAPSHOT-fat.jar -conf ./application-conf.json &
 * */

public class VertxHttpServerVerticle extends AbstractVerticle{	
	
	private static final Logger LOG = LoggerFactory.getLogger(VertxHttpServerVerticle.class);
	private RecommendVerticle recommendVerticle;
	private OrderVerticle personalisationVerticle;
	private ClipVerticle clipVerticle; 
	private Connection connection;
	
	@Override
	public void start() throws IOException  {
		LOG.info("SERVER START");
		
		VertxOptions options = new VertxOptions();
		options.setMaxEventLoopExecuteTime(2000000000);
		options.setWarningExceptionTime(2000000000);
		options.setBlockedThreadCheckInterval(200000000);		
		
		Vertx vertx = Vertx.vertx(options);
		
		Configuration config = new Configuration();
		config.load();
		
		vertx.createHttpServer();	
		
		
		connection = new Connection();
		connection.connect(
				config.getCassandraIP2(),
				config.getCassandraIP3()
				);
		
		recommendVerticle = new RecommendVerticle(connection.getSession());
		personalisationVerticle = new OrderVerticle(connection.getSession());
		clipVerticle = new ClipVerticle(connection.getSession());
		
		Router router = Router.router(vertx);		
		router.route().handler(BodyHandler.create());
		
		router.get("/v1.0/itembase").handler(this::getRecommendItems); //recommend data
		router.post("/v1.0/order").handler(this::addOrder); //구매 입력 
		router.get("/v1.0/order").handler(this::getOrder);//구매 데이터 조회
		router.get("/v1.0/clip").handler(this::getClip); //clip
		router.get("/main").handler(this::getViewPage);
	
		//read port number from application-fong.json configured in jvm arguments. 
		//default port is 8080 unless application-conf doesn't exist
		vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port", 8080));
	}


	
	private void getViewPage(RoutingContext routingContext){		
		final FreeMarkerTemplateEngine engine = FreeMarkerTemplateEngine.create();
		routingContext.put("name", "helloWorld");
		
		engine.render(routingContext, "/templatex/index.ftl", res ->{
			routingContext.response().end(res.result());
		});
	}

	
	private void getRecommendItems(RoutingContext routingContext){
		long startime = System.currentTimeMillis();
		String itemIds = StringUtils.defaultString(routingContext.request().getParam("itemId"));
		String siteNos = StringUtils.defaultString(routingContext.request().getParam("siteNo"), "6001,6002,6003,6004,6009,6100,6200");
		String[] types = StringUtils.split(routingContext.request().getParam("type"), ",");
		int limit = NumberUtils.toInt(routingContext.request().getParam("limit"), 30);		
		LOG.info("itemIDs:{} siteNos:{} types:{} limit:{}", itemIds, siteNos, types, limit);
		validation(routingContext, "itemId", "type", "siteNo", "type"); //see if the received parameters are valid
		
		String json = "";
		try {
			json = recommendVerticle.getRecommendItems(itemIds, siteNos, types, limit);
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("getRecommendItems:{}", e);
		}		
		response(routingContext, json);
		LOG.info("JSON:{}", json);
		long endtime = System.currentTimeMillis();
		LOG.info("getRecommendItems time : {}", endtime-startime);
	}
	
	private void addOrder(RoutingContext routingContext){
		long startime = System.currentTimeMillis();
		String mbrId = routingContext.request().getParam("mbrId");		
		String siteNo = routingContext.request().getParam("siteNo");				
		LOG.info("addOrder mbrId:{} siteNo:{}", mbrId, siteNo);		
		validation(routingContext, "mbrId", "siteNo");
		
		String json = "";
		try {
			json = personalisationVerticle.addOrder(mbrId, siteNo);
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("addOrder:{}", e);
		}
		response(routingContext, json);
		LOG.info("addOrder JSON:{}", json);
		long endtime = System.currentTimeMillis();
		LOG.info("addOrder time : {}", endtime-startime);
	}	
	
	private void getClip(RoutingContext routingContext){	
		long startime = System.currentTimeMillis();
		String brandIds = routingContext.request().getParam("brandId");		
						
		LOG.info("getClip brandId:{} ", brandIds);
		String json = "";
		try {
			json = clipVerticle.getClip(brandIds);
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("getClip:{}", e);
		}
		response(routingContext, json);
		LOG.info("getClip JSON:{}", json);
		long endtime = System.currentTimeMillis();
		LOG.info("getClip time : {}", endtime-startime);
	}	
	
	
	private void getOrder(RoutingContext routingContext){		
		long startime = System.currentTimeMillis();
		String mbrId = routingContext.request().getParam("mbrId");
		LOG.info("getOrder mbrId:{}", mbrId);
		validation(routingContext, "mbrId");
		
		String json = "";
		try {
			json = personalisationVerticle.getOrder(mbrId);
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("getOrder:{}", e);
		}
		response(routingContext, json);
		LOG.info("getOrder JSON:{}", json);		
		long endtime = System.currentTimeMillis();
		LOG.info("getOrder time : {}", endtime-startime);
	}
	

	private void validation(RoutingContext routingContext, String ... params){
		
		for(String param : params){			
			if("itemId".equals(param)){
				String[] itemIds = StringUtils.split(routingContext.request().getParam(param), ",");				
				for(String itemId : itemIds){
					if(StringUtils.isBlank(itemId) || itemId.length() != 13){
						RecommendResponse response = new RecommendResponse();
						response.setCode(400);
						response.setMsg("invalid itemId");
						response(routingContext, Json.encodePrettily(response));
					}
				}
				
			}
			
			if("siteNo".equals(param)){
				String[] siteNos = StringUtils.split(StringUtils.defaultString(routingContext.request().getParam(param), 
						"6001,6004,6009"), ",");
				
				if(siteNos != null){
					for(String siteNo : siteNos){
						if(StringUtils.isNotEmpty(siteNo)){
							if(StringUtils.isBlank(siteNo) || siteNo.length() != 4){
								RecommendResponse response = new RecommendResponse();
								response.setCode(400);
								response.setMsg("invalid site_no");
								response(routingContext, Json.encodePrettily(response));
							}
						}
					}
				}else{
					RecommendResponse response = new RecommendResponse();
					response.setCode(400);
					response.setMsg("siteNo must be existed");
					response(routingContext, Json.encodePrettily(response));
				}
			}
			
			if("mbrId".equals(param)){
				String mbrId = routingContext.request().getParam(param);
				if(mbrId != null){
					if(StringUtils.isBlank(mbrId)){
						RecommendResponse response = new RecommendResponse();
						response.setCode(400);
						response.setMsg("invalid mbr_id");
						response(routingContext, Json.encodePrettily(response));
					}
				}else{
					RecommendResponse response = new RecommendResponse();
					response.setCode(400);
					response.setMsg("mbrId must be existed");
					response(routingContext, Json.encodePrettily(response));
				}
				
			}
			
			if("type".equals(param)){
				String[] types = StringUtils.split(routingContext.request().getParam(param), ",");
				
				if(types != null){
					Map<String, String> typeMap = new HashMap<String, String>();
					typeMap.put("itembase", "itembase");
					typeMap.put("category", "category");
					typeMap.put("brand", "brand");					
					for(String type : types){
						if(StringUtils.isBlank(type) || typeMap.get(type) == null){
							RecommendResponse response = new RecommendResponse();
							response.setCode(400);
							response.setMsg("invalid type");
							response(routingContext, Json.encodePrettily(response));
						}
					}
					
				}else{
					RecommendResponse response = new RecommendResponse();
					response.setCode(400);
					response.setMsg("type must be existed");
					response(routingContext, Json.encodePrettily(response));
				}
				
			}
			
			if("limit".equals(param)){
				String limit = routingContext.request().getParam(param);				
				if(StringUtils.isNumeric(limit) && (Integer.parseInt(limit) > 100 ||
						Integer.parseInt(limit) == 0)){					
					RecommendResponse response = new RecommendResponse();
					response.setCode(400);
					response.setMsg("invalid limit");
					response(routingContext, Json.encodePrettily(response));
				}
			}
		}
	}
	
	
	
	private void response (RoutingContext routingContext, String json){
		routingContext.response()
		.putHeader("content-type", "application/json; charset=utf-8")
		.end(json);
	}
	

	
	@Override
	public void stop() {
		connection.close();
		LOG.info("STOP");
	}
}
