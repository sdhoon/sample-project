package com.ssg.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.ssg.dto.Order;

public class OrderDao {
	
	private static final Logger LOG = LoggerFactory.getLogger(OrderDao.class);
	private ListenableFuture<Session> session;
	
	public OrderDao(ListenableFuture<Session> session){
		this.session = session;
	}
	
	public List<ListenableFuture<ResultSet>> execAsync2(String ids, String query){		
		String[] iDs = StringUtils.split(ids, ",");
		List<ListenableFuture<ResultSet>> listenableFutures = new ArrayList<ListenableFuture<ResultSet>>();		
		
		for(int i=0; i<=iDs.length-1; i++){
			String id = iDs[i];
			ListenableFuture<ResultSet> resultSet = Futures.transform(session,
					new AsyncFunction<Session, ResultSet>(){
				public ListenableFuture<ResultSet> apply (Session session) throws Exception{
					return session.executeAsync(query, id);
				}
			});
			listenableFutures.add(resultSet);
		}
		return Futures.inCompletionOrder(listenableFutures);
	}
	
	public void insertAsync(String id, String siteNo, int ordCnt, String query){
		Futures.transform(session,
				new AsyncFunction<Session, ResultSet>(){
			public ListenableFuture<ResultSet> apply (Session session) throws Exception{
				return session.executeAsync(query, id, siteNo, ordCnt);
			}
		});
		
	}
	
	public Map<String, Order> getOrder(String mbrId) throws InterruptedException, ExecutionException{		
		String query = "SELECT site_no, ord_cnt FROM recommend.orders WHERE mbr_id = ?";		

		Map<String, Order> result = new HashMap<>();
		List<ListenableFuture<ResultSet>> futures = execAsync2(mbrId, query);
		
		int total = 0;
		for(ListenableFuture<ResultSet> future : futures){
			ResultSet rs = future.get();
			List<Row> rows = rs.all();
			for(Row row : rows){	
				String siteNo = row.getString("site_no");
				int ordCnt = row.getInt("ord_cnt");
				total = total + ordCnt;
				
				Order order = new Order();
				order.setOrdCnt(ordCnt);
				order.setSiteNo(siteNo);
				result.put(siteNo, order);
			}
		}
		
		Order order = new Order();
		order.setTotalOrdCnt(total);
		result.put("TOTAL", order);
		
		return result;
	}	
	
	public void addOrder(String mbrId, String siteNos) throws InterruptedException, ExecutionException {
		Map<String, Order> orders = getOrder(mbrId);		
		String query = "INSERT INTO recommend.orders (mbr_id, site_no, ord_cnt) VALUES(?,?,?)";
		
		if(orders.isEmpty()){			
			String[] sites = StringUtils.split(siteNos, ",");
			for(String site : sites){
				insertAsync(mbrId, site, 1, query);
			}
		}else{
			String[] sites = StringUtils.split(siteNos, ",");
			for(String site : sites){
				Order order = orders.get(site);				
				if(order != null){
					insertAsync(mbrId, order.getSiteNo(), order.getOrdCnt() + 1, query);
				}else{
					insertAsync(mbrId, site, 1, query);
				}
			}
		}
	}
}