package com.ssg.dto;

public class Association {
	
	private String consequent;
	private double support;
	private double confidence;
	private double lift;
	
	public String getConsequent() {
		return consequent;
	}
	public void setConsequent(String consequent) {
		this.consequent = consequent;
	}
	public double getSupport() {
		return support;
	}
	public void setSupport(double support) {
		this.support = support;
	}
	public double getConfidence() {
		return confidence;
	}
	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}
	public double getLift() {
		return lift;
	}
	public void setLift(double lift) {
		this.lift = lift;
	}
	

}
