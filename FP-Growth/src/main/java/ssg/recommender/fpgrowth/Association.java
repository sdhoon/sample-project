package ssg.recommender.fpgrowth;

public class Association {
	
	private String antecedent;	
	private String consequent;
	private double support;
	private double confidence;
	private double lift;
	private double conviction;
	
	
	public double getConviction() {
		return conviction;
	}
	public void setConviction(double conviction) {
		this.conviction = conviction;
	}
	public String getAntecedent() {
		return antecedent;
	}
	public void setAntecedent(String antecedent) {
		this.antecedent = antecedent;
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
	public String getConsequent() {
		return consequent;
	}
	public void setConsequent(String consequent) {
		this.consequent = consequent;
	}
}
