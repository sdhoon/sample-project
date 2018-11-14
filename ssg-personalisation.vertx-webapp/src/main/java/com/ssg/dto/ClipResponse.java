package com.ssg.dto;

import java.util.ArrayList;
import java.util.List;

public class ClipResponse {
	
	private int code;
	private List<ClipResult> result = new ArrayList<ClipResult>();
	private String msg;
	
	public int getCode() {
		return code;
	}
	public void setCode(int code) {
		this.code = code;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public List<ClipResult> getResult() {
		return result;
	}
	public void setResult(List<ClipResult> result) {
		this.result = result;
	}	
}