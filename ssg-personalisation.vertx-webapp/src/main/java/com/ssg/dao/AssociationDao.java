package com.ssg.dao;

import java.util.LinkedHashMap;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.ssg.dto.Association;

public class AssociationDao {
	
	private Session session;
	
	public AssociationDao(Session session){
		this.session = session;
	}
	
	public Map<String, Association> selectAssociation(String itemID){
		
		BoundStatement bound = new BoundStatement(session.prepare(
				"SELECT antecedent, consequent, support, confidence, lift FROM association WHERE antecedent = ?"));
		bound.setConsistencyLevel(ConsistencyLevel.ONE);
		
		Map<String, Association> associations = new LinkedHashMap<>();
		
		for (Row row : session.execute(bound.bind(itemID))){
 			Association association = new Association();
 			association.setConsequent(row.getString("consequent"));
 			association.setConfidence(row.getDouble("confidence"));
 			association.setLift(row.getDouble("lift"));
 			association.setSupport(row.getDouble("support"));
 			associations.put(association.getConsequent(), association);
		}
		
		return associations;
	}

}