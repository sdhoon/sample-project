package com.ssg.dto;

import java.util.ArrayList;
import java.util.List;

public class RecommendResponse {
	
	private int code;
	private int arrCnt;
	private List<Item> items = new ArrayList<Item>();
	private String msg = "";

	public int getArrCnt() {
		return arrCnt;
	}
	public void setArrCnt(int arrCnt) {
		this.arrCnt = arrCnt;
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
	public List<Item> getItems() {	
		return items;
	}
	public void setItems(List<Item> items) {
		this.items = items;
	}
}