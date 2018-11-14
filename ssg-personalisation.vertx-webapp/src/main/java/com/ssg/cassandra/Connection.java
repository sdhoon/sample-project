package com.ssg.cassandra;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.google.common.util.concurrent.ListenableFuture;



public class Connection {	
	
	private ListenableFuture<Session> session;
	private Cluster cluster;
	private PoolingOptions poolingOptions;	
	private static final Logger LOG = LoggerFactory.getLogger(Connection.class);
	
	public PoolingOptions getPooling(){
		return poolingOptions;
	}
	
	public Cluster getCluster(){
		return cluster;
	}
	
	public ListenableFuture<Session> getSession() {
		LOG.info("get a cassandra session");
		return session;
	}
	
	public void close(){
		LOG.info("closing cassandra's clusster");		
		cluster.close();
	}

	public void connect(String ip1, String ip2){
		
		cluster = 	Cluster.builder()
					.addContactPoints(ip1, ip2)
					.withQueryOptions(
							new QueryOptions().setConsistencyLevel(
									ConsistencyLevel.ONE
									)
					) //If sufficient replicas exists in the local data center, both reads and writes will default to LOCAL_QUORUM, so it will be strong consistent
					.withSocketOptions(
			                new SocketOptions()
			                		.setReadTimeoutMillis(200)
					)
					.withLoadBalancingPolicy(
			                LatencyAwarePolicy.builder(new RoundRobinPolicy())
			                        .withExclusionThreshold(10.0)
			                        .withScale(100, TimeUnit.MILLISECONDS)
			                        .withRetryPeriod(5, TimeUnit.SECONDS)
			                        .withUpdateRate(100, TimeUnit.MILLISECONDS)
			                        .withMininumMeasurements(50)
			                        .build()
					)
					.withSpeculativeExecutionPolicy( //http://docs.datastax.com/en/developer/java-driver/3.1/manual/speculative_execution/
							new ConstantSpeculativeExecutionPolicy(
									25, // delay before a new execution is launched
									3	// maximum number of executions
							)
					)			
					.build();
			
	      Metadata metadata = cluster.getMetadata();	      
	      LOG.info("Connected to cluster: {}\n", metadata.getClusterName());
	      
	      for ( Host host : metadata.getAllHosts() ) {
	    	  LOG.info("Datatacenter: {}; Host: {}; Rack: {}\n",host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	      session = cluster.connectAsync();
	}

}
