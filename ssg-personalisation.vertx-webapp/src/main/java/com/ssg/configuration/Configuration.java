package com.ssg.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {
	
	private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
	private String cassandraIP1;
	private String cassandraIP2;
	private String cassandraIP3;
	
	public String getCassandraIP1() {
		return cassandraIP1;
	}

	public void setCassandraIP1(String cassandraIP1) {
		this.cassandraIP1 = cassandraIP1;
	}

	public String getCassandraIP2() {
		return cassandraIP2;
	}

	public String getCassandraIP3() {
		return cassandraIP3;
	}

	public void setCassandraIP3(String cassandraIP3) {
		this.cassandraIP3 = cassandraIP3;
	}

	public void setCassandraIP2(String cassandraIP2) {
		this.cassandraIP2 = cassandraIP2;
	}
	
	private String getPropertyName(){
		if(System.getProperty("container.name") == null 
				|| System.getProperty("container.name").contains("dev")){
			return "config.properties.dev";
		}else if(System.getProperty("container.name") == null 
				|| System.getProperty("container.name").contains("qa")){
			return "config.properties.qa";
		}else if(System.getProperty("container.name") == null 
				|| System.getProperty("container.name").contains("stg")){
			return "config.properties.stg";
		}else{
			return "config.properties";
		}
	}

	public void load() throws IOException{
		Properties prop = new Properties();
		InputStream input = getClass().getClassLoader().getResourceAsStream(getPropertyName());
		try {
			prop.load(input);
			this.cassandraIP1 = prop.getProperty("cassandraIP1");
			this.cassandraIP2 = prop.getProperty("cassandraIP2");
			this.cassandraIP3 = prop.getProperty("cassandraIP3");
		} finally{
			try {
				if(input != null){
					input.close();
				}
			} catch (IOException e) {
				LOG.error("config file error : {}", e);
			}
		}
	}
}
