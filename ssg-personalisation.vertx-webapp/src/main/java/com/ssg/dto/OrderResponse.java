package com.ssg.dto;

import java.util.ArrayList;
import java.util.List;

public class OrderResponse {
	
	private int code;	
	private String mbrId;
	private int arrCnt;
	private int totOrdCnt;
	private String msg;
	private List<Order> ordCntBySites = new ArrayList<Order>();
	
	public List<Order> getOrdCntBySites() {
		return ordCntBySites;
	}

	public void setOrdCntBySites(List<Order> ordCntBySites) {
		this.ordCntBySites = ordCntBySites;
	}
	
	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMbrId() {
		return mbrId;
	}

	public void setMbrId(String mbrId) {
		this.mbrId = mbrId;
	}

	

	public int getTotOrdCnt() {
		return totOrdCnt;
	}

	public void setTotOrdCnt(int totOrdCnt) {
		this.totOrdCnt = totOrdCnt;
	}

	public int getArrCnt() {
		return arrCnt;
	}

	public void setArrCnt(int arrCnt) {
		this.arrCnt = arrCnt;
	}
}
