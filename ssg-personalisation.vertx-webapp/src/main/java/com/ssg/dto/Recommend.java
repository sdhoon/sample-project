package com.ssg.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class Recommend{
	private String recomItem;
	private String type;
	private String siteNo;
	
	@JsonInclude(Include.NON_DEFAULT)
	private String rank;

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}

	public String getSiteNo() {
		return siteNo;
	}

	public void setSiteNo(String siteNo) {
		this.siteNo = siteNo;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	



	public String getRecomItem() {
		return recomItem;
	}

	public void setRecomItem(String recomItem) {
		this.recomItem = recomItem;
	}


	
	@Override
	public int hashCode(){
		return recomItem.hashCode();
	}
	
	@Override
	public boolean equals(Object obj){
		
		if(obj instanceof Recommend){
			
			Recommend temp = (Recommend) obj;
			
			if(this.recomItem.equals(temp.getRecomItem())){
				return true;
			}			
		}
		return false;
	
	}
}