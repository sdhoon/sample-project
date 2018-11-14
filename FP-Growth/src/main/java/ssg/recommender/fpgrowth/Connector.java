package ssg.recommender.fpgrowth;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.UUIDs;


public class Connector{
	
	private Cluster cluster;
	private Session session;
	//private PoolingOptions poolingOptions;
	private PreparedStatement pstmtInsert;
	private PreparedStatement pstmtSelect;
	
	private static final Logger LOG = LoggerFactory.getLogger(Connector.class);
	
	
	public void connection(){
		
		cluster = 	Cluster
					.builder()
					.addContactPoints("10.203.7.46")
					.withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE))//if sufficient replicas do not exists in the local data center, the consistency level will downgrade to either ONE, TWO, or THREE.
					.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("DC1"))) //it will continue to try to satisfy consistency level using only local nodes if possible, avoiding unnecessay trips to the remote data centers as long as the local data center can fulfill the downgraded consistency level.
					//.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)) //If sufficient replicas exists in the local data center, both reads and writes will default to LOCAL_QUORUM, so it will be strong consistent			
					.build();
		
		//Metadata metaData = cluster.getMetadata();
		
		session = cluster.connect("recommend"); //get keyspaces according to the container name of was
	}
	
	public void close(){
		cluster.close();
	}
	
	
	
	/**
	 * do not create the same preparedStatement, which will degrade performance.
	 * this method creates a preparedStatement.
	 * @return PreparedStatement
	 */
	public PreparedStatement insert(){
		if(pstmtInsert == null){			
			pstmtInsert = session.prepare(
					"INSERT INTO association (ANTECEDENT, CONSEQUENT, SUPPORT, CONFIDENCE, LIFT, CONVICTION, REG_DTS) VALUES(?, ?, ?, ?, ?, ?, ?)");
		}
		return pstmtInsert;
	}
	
	/**
	 * do not create the same preparedStatement, which will degrade performance.
	 * this method creates a preparedStatement.
	 * @return PreparedStatement
	 */
	public PreparedStatement select(){
		if(pstmtSelect == null){			
			pstmtSelect = session.prepare(
					"SELECT ANTECEDENT, CONSEQUENT, SUPPORT, CONFIDENCE, LIFT FROM ASSOCIATION WHERE ANTECEDENT = ? AND CONSEQUENT = ?");
		}
		return pstmtSelect;
	}
	
	
	public Association getAssociation(String antecedent, String consequent){
		BoundStatement bound = new BoundStatement(select());
		bound.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		
		Association association = new Association();
		for (Row row : session.execute(bound.bind(antecedent, consequent))){
			association.setAntecedent(row.getString("ANTECEDENT"));
			association.setConsequent(row.getString("CONSEQUENT"));
			association.setSupport(row.getDouble("SUPPORT"));
			association.setConfidence(row.getDouble("CONFIDENCE"));
			association.setLift(row.getDouble("LIFT"));
			
			LOG.info("DATA READ ANTECEDENT:{} CONSEQUENT:{} SUPPORT:{} CONFIDENCE:{} LIFT:{}",
					association.getAntecedent(), association.getConsequent(), association.getSupport(), association.getConfidence(), association.getLift());
		}	
		
		return association;
	}
	
	
	public void insertAssociation(Association association) throws ParseException{
		BoundStatement boundStatement = new BoundStatement(insert());
		boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		
		session.execute(boundStatement.bind(association.getAntecedent(),
				association.getConsequent(),
				association.getSupport(),
				association.getConfidence(),
				association.getLift(),
				association.getConviction(),
				UUIDs.timeBased()));
	}
	
}