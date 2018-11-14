package com.ssg.clip;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.ssg.dao.ClipDao;
import com.ssg.dto.ClipResponse;
import com.ssg.dto.ClipResult;

import io.vertx.core.json.Json;

public class ClipVerticle {
	
	private ClipDao dao;	
	private ListenableFuture<Session> session;
	private static final Logger LOG = LoggerFactory.getLogger(ClipVerticle.class);	
	
	public ClipVerticle(ListenableFuture<Session> session){
		this.session = session;
		this.dao = new ClipDao(this.session);
	}
	
	public String getClip(String brandIds) throws InterruptedException, ExecutionException{
		Map<String, ClipResult> clipMap = dao.getClipData(brandIds);		
		long start2 = System.currentTimeMillis();
		String json = Json.encodePrettily(setResponse(clipMap));
		long end2 = System.currentTimeMillis();
		LOG.info("json time : {}", end2-start2);
		return json;
	}
	
	public ClipResponse setResponse(Map<String, ClipResult> clipMap){
		List<ClipResult> clipResults = new ArrayList<ClipResult>();
		for(String key : clipMap.keySet()){
			ClipResult clipResult = clipMap.get(key);
			clipResults.add(clipResult);
		}		
		ClipResponse response = new ClipResponse();
		response.setCode(200);
		response.setResult(clipResults);
		response.setMsg("");	
		return response;
	}
}
