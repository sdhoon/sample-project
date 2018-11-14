package com.ssg.recommendation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.ssg.dao.RecommendDao;
import com.ssg.dto.Item;
import com.ssg.dto.Recommend;
import com.ssg.dto.RecommendResponse;

import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

public class RecommendVerticle {
	
	private static final Logger LOG = LoggerFactory.getLogger(RecommendVerticle.class);	
	private ListenableFuture<Session> session;
	
	public RecommendVerticle(ListenableFuture<Session> session){
		this.session = session;
	}
	
	public String getRecommendItems(String itemIds, String siteNos, String[] types, int limit) throws InterruptedException, ExecutionException{
		Map<String, Item> result = new LinkedHashMap<String, Item>();
		RecommendDao recommendDao = new RecommendDao(session);
		
		String[] ids = StringUtils.split(itemIds, ",");
		for(String type : types){			
			StringBuilder inSufficientItems = new StringBuilder();
			
			for(String id : ids){				
				if(result.get(id) == null){
					inSufficientItems.append(id).append(",");
					
				}else{
					int size = result.get(id).getRecomItems().size();
					
					if(size < limit){
						inSufficientItems.append(id).append(",");
					}
				}
			}
			
			if(inSufficientItems.length() > 0){
				inSufficientItems = inSufficientItems.replace(inSufficientItems.length() - 1, inSufficientItems.length(), "");
			}
			
			
			Map<String, Item> data = new LinkedHashMap<String, Item>();
			if("itembase".equals(type)){
				long start_itembase = System.currentTimeMillis();
				data = recommendDao.selectItemBase(inSufficientItems.toString(), siteNos, limit);
				long end_itembase = System.currentTimeMillis();
				LOG.info("itembase time : {}", end_itembase-start_itembase);
			}
			
			if("category".equals(type)){
				long start_category = System.currentTimeMillis();
				data = recommendDao.selectBrandAsync("std_ctg_id", type, inSufficientItems.toString(), siteNos, "itemrank_category_", limit);
				long end_category = System.currentTimeMillis();
				LOG.info("category time : {}", end_category-start_category);
			}
			
			if("brand".equals(type)){
				long start_brand = System.currentTimeMillis();
				data = recommendDao.selectBrandAsync("brand_id", type, inSufficientItems.toString(), siteNos, "itemrank_brand_", limit);
				long end_brand = System.currentTimeMillis();
				LOG.info("brand time : {}", end_brand-start_brand);
			}
			
			setData(data, result, limit);
		}
		
		//set response
		//long start2 = System.currentTimeMillis();
		String json = Json.encodePrettily(setResponse(result));
		//long end2 = System.currentTimeMillis();
		//LOG.info("json time : {}", end2-start2);
		return json;
	}
	
	
	private Map<String, Item> setData (Map<String, Item> intermediateResult, Map<String, Item> result, int limit){		
		for(String key : intermediateResult.keySet()){
			Item items = intermediateResult.get(key);
			Set<Recommend> recommends = items.getRecomItems();
			
			//get data from a result map
			Item item = new Item();
			if(result.get(key) != null){	
				item = result.get(key);
				Set<Recommend> resultItems = item.getRecomItems();
				Set<Recommend> linkedResultItems = new LinkedHashSet<Recommend>();
				linkedResultItems.addAll(resultItems);				
				int cnt = linkedResultItems.size();
				for(Recommend recommend : recommends){
					cnt++;
					linkedResultItems.add(recommend);
					if(limit == cnt) break;					
				}				
				item.setRecomItemsCnt(linkedResultItems.size());
				item.setRecomItems(linkedResultItems);
				result.put(key, item);
			}else{
				//NEW
				result.put(key, items);
			}
		}
		return result;
	}
	
	public RecommendResponse setResponse(Map<String, Item> result){
		//makes response class
		List<Item> items = new ArrayList<Item>();
		for(String key : result.keySet()){
			Item item = result.get(key);
			items.add(item);
		}		
		RecommendResponse response = new RecommendResponse();
		response.setCode(200);
		response.setArrCnt(items.size());
		response.setItems(items);			
		return response;
	}
	
	public void getViewPage(RoutingContext routingContext){		
		final FreeMarkerTemplateEngine engine = FreeMarkerTemplateEngine.create();
		routingContext.put("name", "helloWorld");		
		engine.render(routingContext, "/templatex/index.ftl", res ->{
			routingContext.response().end(res.result());
		});
	}
}