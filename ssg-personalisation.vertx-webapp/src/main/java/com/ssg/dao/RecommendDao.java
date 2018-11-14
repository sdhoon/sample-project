package com.ssg.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.ssg.dto.Item;
import com.ssg.dto.Recommend;

public class RecommendDao{
	
	private static final Logger LOG = LoggerFactory.getLogger(RecommendDao.class);
	private ListenableFuture<Session> session;	
	
	public RecommendDao(ListenableFuture<Session> session) {
		this.session = session;
	}
	
	public static String makeInStatement(String value){
		String[] values = StringUtils.split(value, ",");
		StringBuilder inStatement = new StringBuilder();
		for(String s : values){
			inStatement.append("\'").append(s).append("\'").append(",");
		}
		inStatement = inStatement.replace(inStatement.length() - 1, inStatement.length(), "");
		return inStatement.toString();
	}	
	
	
	public List<ListenableFuture<ResultSet>> execAsync2(String itemIds, String siteNos, String query){		
		String[] iDs = StringUtils.split(itemIds, ",");
		String[] sites = StringUtils.split(siteNos, ",");
		List<ListenableFuture<ResultSet>> listenableFutures = new ArrayList<ListenableFuture<ResultSet>>();		
		List<String> faillist = new ArrayList<String>();
		
		for(int i=0; i<=iDs.length-1; i++){
			for(int j=0; j<=sites.length-1; j++){
				String id = iDs[i];
				String siteNo = sites[j];
				//LOG.info("siteNo : {}", siteNo);
				try {
					ListenableFuture<ResultSet> resultSet = Futures.transform(session,
							new AsyncFunction<Session, ResultSet>(){
						public ListenableFuture<ResultSet> apply (Session session) throws Exception{
							return session.executeAsync(query, id, siteNo);
						}
					});
					listenableFutures.add(resultSet);
				} catch (OperationTimedOutException e) {
					faillist.add(id+","+siteNo);
				}
				
			}
		}
		LOG.debug("listenableFutures count : {} / faillist's count : {}", listenableFutures.size(), faillist.size());
		if (faillist!=null && faillist.size() > 0) {
			
			for (String connectionfail : faillist) {
				String id = connectionfail.split(",")[0];
				String siteNo = connectionfail.split(",")[1];
				
				ListenableFuture<ResultSet> resultSet = Futures.transform(session,
						new AsyncFunction<Session, ResultSet>(){
					public ListenableFuture<ResultSet> apply (Session session) throws Exception{
						return session.executeAsync(query, id, siteNo);
					}
				});
				listenableFutures.add(resultSet);
			}
			LOG.debug("retry listenableFutures count : {} ", listenableFutures.size());
		}
		
		return Futures.inCompletionOrder(listenableFutures);
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
	
	private static List<ResultSetFuture> sendQueries(Session session, String query, Object[] partitionKeys) {
	    List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(partitionKeys.length);
	    for (Object partitionKey : partitionKeys)
	        futures.add(session.executeAsync(query, partitionKey));
	    return futures;
	}
	
	public static Future<List<ResultSet>> queryAllAsList(Session session, String query, Object[] partitionKeys) {
	    List<ResultSetFuture> futures = sendQueries(session, query, partitionKeys);
	    return Futures.successfulAsList(futures);
	}
	
	public Map<String, Item> selectBrandAsync(String id, String type, String itemIDs, String siteNos, String tableName, int limit) throws InterruptedException, ExecutionException{		
		String code = selectCode("ITEM_DIV_CD");		
		String query = "SELECT " + id + ", item_id FROM recommend.item_" + code + " WHERE item_id = ?";
		//LOG.debug("selectBrandAsync1 QUERY : {}", query);
			
		//Future<List<ResultSet>> future2 = execAsync(itemIDs, query);	
		List<ListenableFuture<ResultSet>> futures2 = execAsync2(itemIDs, query);
		StringBuilder ids = new StringBuilder();
		Map<String, String> idMap = new HashMap<String, String>();
		Map<String, Item> best = new LinkedHashMap<>();
		
		try{
			for(ListenableFuture<ResultSet> future : futures2){
				ResultSet rs = future.get();
				List<Row> rows = rs.all();
				
				for(Row row : rows){
					String itemId = row.getString("item_id");
					String idVal = row.getString(id);
					
					idMap.put(idVal, itemId);
					ids.append(idVal).append(",");
					//LOG.info("itemId:{}, brandID:{}", itemId, brandId);
				}
				
			}
			
			
			if(ids.length() > 0){
				ids = ids.replace(ids.length() - 1, ids.length(), "");
			}
		}catch(Exception e){
			return best;
		}
		
		
		
		code = selectCode(id.equals("std_ctg_id") ? "BEST_EFF_DIV_CD" : "RCMD_EFF_DIV_CD");
		query = "SELECT " + id + ", item_id, rank, site_no FROM recommend."  + tableName + code + " WHERE " + id + " = ? AND site_no = ? LIMIT " + limit;
		//LOG.debug("selectBrandAsync2 QUERY : {}, brandId : {}", query);
		
		List<ListenableFuture<ResultSet>> futures = execAsync2(ids.toString(), siteNos, query);
		
		
		try{
			for(ListenableFuture<ResultSet> future : futures){
				ResultSet rs = future.get();
				List<Row> rows = rs.all();			
				for(Row row : rows){					
					String recomId = row.getString("item_id");
					String idVal = row.getString(id);
					int rank = row.getInt("rank");
					String siteNo = row.getString("site_no");
					String reqItemId = idMap.get(idVal);					
					//LOG.debug("recomId : {}, id : {}, idVal : {}, rank : {}, siteNo : {}, reqItemId : {}", recomId, id, idVal, rank, siteNo, reqItemId);
					
					if(best.size() > 0 &&
							best.get(reqItemId) != null){
						
						Recommend recommend = new Recommend();
						recommend.setRecomItem(recomId);
						recommend.setRank(String.valueOf(rank));
						recommend.setType(type);
						recommend.setSiteNo(siteNo);
						
						Item items = best.get(reqItemId);
						Set<Recommend> bests = items.getRecomItems();
						if(bests.size() >= limit) continue;
						bests.add(recommend);
						
						items.setItemId(reqItemId);
						items.setRecomItemsCnt(bests.size()); 
						items.setRecomItems(sortSetByRankAsc(bests)); //sort By Rank
										
						best.put(reqItemId, items);				
					}else{
						//NEW				
						Recommend recommend = new Recommend();
						recommend.setRecomItem(recomId);
						recommend.setRank(String.valueOf(rank));
						recommend.setType(type);
						recommend.setSiteNo(siteNo);
						
						Set<Recommend> bests = new LinkedHashSet<Recommend>();
						bests.add(recommend);
						
						Item items = new Item();
						items.setItemId(reqItemId);
						items.setRecomItemsCnt(bests.size());
						items.setRecomItems(sortSetByRankAsc(bests)); //sort By Rank
						
						best.put(reqItemId, items);
					}
					
				}
			}
		}catch(Exception e){
			return best;
		}
		
		return best;
	}
	
	
	public Map<String, Item> selectCategoryAsync(String type, String itemIDs, String siteNos, int limit) throws InterruptedException, ExecutionException{		
		String code = selectCode("RCMD_EFF_DIV_CD");
		String query = "SELECT item_id, recommend_item_id, score, site_no FROM recommend.itemrank_category_" + code + " WHERE item_id = ? AND site_no = ? LIMIT " + limit;
		//LOG.debug("selectCategoryAsync QUERY : {}", query);
		
		List<ListenableFuture<ResultSet>> futures = execAsync2(itemIDs, siteNos, query);
		Map<String, Item> best = new LinkedHashMap<>();
	
		try{
			for (ListenableFuture<ResultSet> future : futures) {
				ResultSet rs = future.get();
				List<Row> rows = rs.all();
				for(Row row : rows){				
					String itemId = row.getString("item_id");
					String recomId = row.getString("recommend_item_id");			
					Double score = row.getDouble("score");
					String siteNo = row.getString("site_no");
					//LOG.debug("itemId : {}, recomId : {}, score : {}, siteNo : {} ", itemId,  recomId, score, siteNo);
					
					if(best.size() > 0 &&
							best.get(itemId) != null){					
						Recommend recommend = new Recommend();
						recommend.setRecomItem(recomId);
						recommend.setRank(String.valueOf(score));
						recommend.setType(type);
						recommend.setSiteNo(siteNo);
						
						Item items = best.get(itemId);
						Set<Recommend> bests = items.getRecomItems();
						if(bests.size() >= limit) continue;
						bests.add(recommend);
						
						items.setItemId(itemId);
						items.setRecomItemsCnt(bests.size()); 
						items.setRecomItems(sortSetByRankDesc(bests)); //sort By Score
										
						best.put(itemId, items);				
					}else{
						//NEW				
						Recommend recommend = new Recommend();
						recommend.setRecomItem(recomId);
						recommend.setRank(String.valueOf(score));
						recommend.setType(type);
						recommend.setSiteNo(siteNo);
						
						Set<Recommend> bests = new LinkedHashSet<Recommend>();
						bests.add(recommend);
						
						Item items = new Item();
						items.setItemId(itemId);
						items.setRecomItemsCnt(bests.size());
						items.setRecomItems(sortSetByRankDesc(bests)); //sort By Rank
						
						best.put(itemId, items);
					}
				}
			}
		}catch(Exception e){
			return best;
		}
	
	return best;
}
	
	
	/**
	 * Sort by Recommend score
	 * @param recommends
	 * @return
	 */
	private Set<Recommend> sortSetByRankAsc(Set<Recommend> recommends){
		
		List<Recommend> list = new ArrayList<Recommend>();
		list.addAll(recommends);
		
		Collections.sort(list, new Comparator<Recommend>(){
			@Override
			public int compare(Recommend obj1, Recommend obj2) {
				if(Double.valueOf(obj1.getRank()) > Double.valueOf(obj2.getRank())){
					return 1;
				}else if(Double.valueOf(obj1.getRank()) < Double.valueOf(obj2.getRank())){
					return -1;
				}else{
					return 0;
				}
			}
		});		
		return new LinkedHashSet<Recommend>(list);
	}
	
	/**
	 * Sort by Recommend score
	 * @param recommends
	 * @return
	 */
	private Set<Recommend> sortSetByRankDesc(Set<Recommend> recommends){
		
		List<Recommend> list = new ArrayList<Recommend>();
		list.addAll(recommends);
		
		Collections.sort(list, new Comparator<Recommend>(){
			@Override
			public int compare(Recommend obj1, Recommend obj2) {
				if(Double.valueOf(obj1.getRank()) < Double.valueOf(obj2.getRank())){
					return 1;
				}else if(Double.valueOf(obj1.getRank()) > Double.valueOf(obj2.getRank())){
					return -1;
				}else{
					return 0;
				}
			}
		});		
		return new LinkedHashSet<Recommend>(list);
	}
	
	public String selectCode(String div) throws InterruptedException, ExecutionException{
		String query = "SELECT div, code FROM recommend.table_division WHERE div = ?";
		
		String code = "";
		List<ListenableFuture<ResultSet>> futures = execAsync2(div, query);
		
		for(ListenableFuture<ResultSet> future : futures){
			ResultSet rs = future.get();
			List<Row> rows = rs.all();
			for(Row row : rows){
				code = row.getString("code");
				//LOG.info("selectCode : {}", code);
			}
		}
		return code;
	}

	
	public Map<String, Item> selectItemBase(String itemIDs, String siteNos, int limit) throws InterruptedException, ExecutionException {	
		
		String query = "SELECT item_id, recommend_item_id, site_no, score FROM recommend.itembase2 WHERE item_id = ? "
				+ "AND site_no = ? "
				+ "AND method = 'CLICK' LIMIT " + (limit + 20);
		
		Map<String, Item> itemBases = new LinkedHashMap<>();		
		Map<String, Item> rtnItemBases = new LinkedHashMap<>();	
		
		List<ListenableFuture<ResultSet>> futures = execAsync2(itemIDs, siteNos, query);
		try {
			for(ListenableFuture<ResultSet> future : futures){
				ResultSet rs = future.get();
				List<Row> rows = rs.all();
				for(Row row : rows){
					String itemId = row.getString("item_id");
					String recommnedItemId = row.getString("recommend_item_id");
					Double score = row.getDouble("score");
					String siteNo = row.getString("site_no");					
					//LOG.info("itemId : {}, recommnedItemId : {}, score : {}, siteNo : {}", itemId, recommnedItemId, score, siteNo);
					
					if(!itemBases.isEmpty() && 
							itemBases.get(itemId) != null){
						
						Item items = itemBases.get(itemId); 				
						
						Recommend recommend = new Recommend();
						recommend.setRecomItem(recommnedItemId);
						recommend.setRank(String.valueOf(score));
						recommend.setType("itembase");
						recommend.setSiteNo(siteNo);
						
						Set<Recommend> recommends = items.getRecomItems();
						recommends.add(recommend);
						
						items.setItemId(itemId);
						items.setRecomItemsCnt(recommends.size()); 
						items.setRecomItems(recommends);
						
						itemBases.put(itemId, items);
						
					}else{
						//NEW				
						Recommend recommend = new Recommend();
						recommend.setRecomItem(recommnedItemId);
						recommend.setRank(String.valueOf(score));
						recommend.setType("itembase");
						recommend.setSiteNo(siteNo);
						
						Set<Recommend> recommends = new HashSet<Recommend>();
						recommends.add(recommend);
						
						Item items = new Item();
						items.setItemId(itemId);
						items.setRecomItemsCnt(recommends.size());
						items.setRecomItems(recommends);
						
						itemBases.put(itemId, items);
					}
				}
			}
			
			//sort and limit data
			rtnItemBases = sortItemBase(rtnItemBases, limit);
			
		}catch(Exception e){
			return rtnItemBases;
		}
 		return rtnItemBases;		
	}
	
	public Map<String, Item> sortItemBase(Map<String, Item> itemBases, int limit){
		
		Map<String, Item> rtnItemBases = new LinkedHashMap<>();	
		
		//sort and limit data
		for(String key : itemBases.keySet()){				
			Item items = itemBases.get(key);
			Set<Recommend> sortedRecommends = sortSetByRankDesc(items.getRecomItems()); //sort
			Set<Recommend> rtnRecommend = new LinkedHashSet<Recommend>();				
			for(Recommend recommend : sortedRecommends){
				rtnRecommend.add(recommend);
				
				if(rtnRecommend.size() >= limit) {//limit
					break;
				}
			}
			
			items.setRecomItemsCnt(rtnRecommend.size()); 
			items.setRecomItems(rtnRecommend);
			rtnItemBases.put(key, items);
		}
		
		return rtnItemBases;
	}
}