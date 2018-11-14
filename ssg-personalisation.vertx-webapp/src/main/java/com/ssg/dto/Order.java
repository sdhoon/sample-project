package com.ssg.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Order {
	
	private String siteNo;
	private int ordCnt;
	
	@JsonIgnore
	private int totalOrdCnt;
	
	public int getTotalOrdCnt() {
		return totalOrdCnt;
	}
	public void setTotalOrdCnt(int totalOrdCnt) {
		this.totalOrdCnt = totalOrdCnt;
	}
	public String getSiteNo() {
		return siteNo;
	}
	public void setSiteNo(String siteNo) {
		this.siteNo = siteNo;
	}
	public int getOrdCnt() {
		return ordCnt;
	}
	public void setOrdCnt(int ordCnt) {
		this.ordCnt = ordCnt;
	}
}