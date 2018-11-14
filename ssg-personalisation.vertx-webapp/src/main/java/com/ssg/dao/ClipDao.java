package com.ssg.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import com.ssg.dto.Clip;
import com.ssg.dto.ClipResult;
import com.ssg.dto.Kind;

public class ClipDao {
	
private ListenableFuture<Session> session;
private static final Logger LOG = LoggerFactory.getLogger(ClipDao.class);
private static final int NEW_LIMIT 		= 10;
private static final int BRAND_LIMIT 	= 10;
private static final int BEST_LIMIT 	= 10;

	private boolean isLimit(String kind, String brandId, Map<String, Kind> kindCntMap){		
		boolean isLimit = false;		
		Kind kindClass = kindCntMap.get(brandId);
		
		if(kindClass != null){
			if("new".equals(kind)){				
				if(NEW_LIMIT <= kindClass.getNewCnt()){
					isLimit = true;
				}
			}else if("best".equals(kind)){				
				if(BEST_LIMIT <= kindClass.getBestCnt()){
					isLimit = true;
				}
			}else if("brand".equals(kind)){
				if(BRAND_LIMIT <= kindClass.getBrandCnt()){
					isLimit = true;
				}
			}
		}
		return isLimit;
	}
	
	private void addKindCnt(String kind, String brandId, Map<String, Kind> kindCntMap){
		Kind kindClass = kindCntMap.get(brandId);		
		if(kindClass != null){
			addKindCnt(kind, kindClass);
		}else{
			kindClass = new Kind();
			addKindCnt(kind, kindClass);
		}
		kindCntMap.put(brandId, kindClass);
	}
	
	private void addKindCnt(String kind, Kind kindClass){
		if("new".equals(kind)){
			kindClass.setNewCnt(kindClass.getNewCnt() + 1);
		}else if("best".equals(kind)){
			kindClass.setBestCnt(kindClass.getBestCnt() + 1);
		}else if("brand".equals(kind)){
			kindClass.setBrandCnt(kindClass.getBrandCnt() + 1);
		}	
	}

	
	public ClipDao(ListenableFuture<Session> session){
		this.session = session;
	}
	
	private List<ListenableFuture<ResultSet>> execAsync2(String ids, String query){		
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
	
	public Map<String, ClipResult> getClipData(String brandIds) throws InterruptedException, ExecutionException{		
		String query = "SELECT brand_id, kind, id, rank FROM clip.clip WHERE brand_id = ?";
		List<ListenableFuture<ResultSet>> futures = execAsync2(brandIds, query);
		Map<String, ClipResult> clipMap = new LinkedHashMap<>();
		Map<String, Kind> kindCntMap = new HashMap<String, Kind>();
		
				
		for(ListenableFuture<ResultSet> future : futures){
			ResultSet rs = future.get();
			List<Row> rows = rs.all();
			for(Row row : rows){					
				String brandId = row.getString("brand_id");
				String id = row.getString("id");
				String rank = String.valueOf(row.getInt("rank"));					
				String kind = row.getString("kind");					
				
				if(isLimit(kind, brandId, kindCntMap)) continue;					
				
				if(clipMap.size() > 0 &&
						clipMap.get(brandId) != null){
					
					Clip clip = new Clip();
					clip.setId(id);
					clip.setRank(rank);
					clip.setKind(kind);
					
					ClipResult clipResult = clipMap.get(brandId);
					List<Clip> clips = clipResult.getIds();
					clips.add(clip);
					
					clipResult.setBrandId(brandId);
					clipResult.setCnt(clips.size());
					clipResult.setIds(clips);
					
					addKindCnt(kind, brandId, kindCntMap);
					
					clipMap.put(brandId, clipResult);
					
				}else{
					
					Clip clip = new Clip();
					clip.setId(id);
					clip.setRank(rank);
					clip.setKind(kind);
					
					List<Clip> clips = new ArrayList<Clip>();
					clips.add(clip);
					
					ClipResult clipResult = new ClipResult();
					
					clipResult.setBrandId(brandId);
					clipResult.setCnt(clips.size());
					clipResult.setIds(clips);
					
					addKindCnt(kind, brandId, kindCntMap);
					
					clipMap.put(brandId, clipResult);
					
				}
			}				
		}
		
		return clipMap;
	}
}
