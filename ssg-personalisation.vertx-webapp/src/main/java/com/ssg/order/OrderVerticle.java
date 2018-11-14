package com.ssg.order;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.ssg.dao.OrderDao;
import com.ssg.dto.Order;
import com.ssg.dto.OrderResponse;

import io.vertx.core.json.Json;

public class OrderVerticle {
	
	//private static final Logger LOG = LoggerFactory.getLogger(OrderVerticle.class);
	private OrderDao dao;	
	private ListenableFuture<Session> session;
	
	public OrderVerticle(ListenableFuture<Session> session){
		this.session = session;
		this.dao = new OrderDao(this.session);
	}
	
	public String getOrder(String mbrId) throws InterruptedException, ExecutionException{
		Map<String, Order> result = dao.getOrder(mbrId);
		OrderResponse response = setResponse(result, mbrId);
		return Json.encodePrettily(response);
	}
	
	public String addOrder(String mbrId, String siteNos) throws InterruptedException, ExecutionException{
		dao.addOrder(mbrId, siteNos);
		return getOrder(mbrId);
	}
	
	public OrderResponse setResponse( Map<String, Order> result, String mbrId){
		
		List<Order> orders = new ArrayList<Order>();
		for(String key : result.keySet()){
			if(!"TOTAL".equals(key)){
				orders.add(result.get(key));
			}
		}
		
		OrderResponse response  = new OrderResponse();
		response.setCode(200);
		response.setMbrId(mbrId);
		response.setArrCnt(orders.size());
		response.setTotOrdCnt(result.get("TOTAL").getTotalOrdCnt());
		response.setMsg("");
		response.setOrdCntBySites(orders);
		return response;
	}
}