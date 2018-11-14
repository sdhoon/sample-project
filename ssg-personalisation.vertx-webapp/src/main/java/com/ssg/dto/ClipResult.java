package com.ssg.dto;

import java.util.ArrayList;
import java.util.List;

public class ClipResult {
	
	private String brandId;
	private int cnt;	
	private List<Clip> ids = new ArrayList<Clip>();
	
	public String getBrandId() {
		return brandId;
	}
	public void setBrandId(String brandId) {
		this.brandId = brandId;
	}	
	public int getCnt() {
		return cnt;
	}
	public void setCnt(int cnt) {
		this.cnt = cnt;
	}
	public List<Clip> getIds() {
		return ids;
	}
	public void setIds(List<Clip> ids) {
		this.ids = ids;
	}
	
	
}