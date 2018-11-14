package com.ssg.dto;

import java.util.HashSet;
import java.util.Set;

public class Item {	
	
	private String itemId;
	private int recomItemsCnt;
	private Set<Recommend> recomItems = new HashSet<Recommend>();	

	public int getRecomItemsCnt() {
		return recomItemsCnt;
	}

	public void setRecomItemsCnt(int recomItemsCnt) {
		this.recomItemsCnt = recomItemsCnt;
	}	

	public Set<Recommend> getRecomItems() {
		return recomItems;
	}

	public void setRecomItems(Set<Recommend> recomItems) {
		this.recomItems = recomItems;
	}

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}	

	
}
