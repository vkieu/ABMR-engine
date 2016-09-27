package com.smartfocus.report;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

import com.smartfocus.demo.BehaviourImpl;

@ComponentScan("com.smartfocus.demo")
@PropertySource(value="file:global.properties")
@SpringBootApplication
public class ABMReport {

	@Value("${hbase.host.url:localhost}")
	private String hbaseHostUrl;	
	@Value("${hbase.zookeeper.host:localhost}")	
	private String zookeeperHost;	
	@Value("${hbase.zookeeper.port:2181}")
	private int zookeeperPort;	
	@Value("${hbase.connection.timeout:12000}")
	private int timeout;
	
	@Autowired
	private BehaviourImpl behaviourImpl;
	
	private Configuration getHBaseConf() {
		Configuration conf = HBaseConfiguration.create();				
		conf.setInt("timeout", timeout);
		conf.set("hbase.master", hbaseHostUrl);
		conf.set("hbase.zookeeper.quorum", zookeeperHost);
		conf.setInt("hbase.zookeeper.property.clientPort", zookeeperPort);			
		return conf;
	}
		
	public static void main(String[] args) throws Exception {
		
		ApplicationContext ctx = SpringApplication.run(ABMReport.class, args);		
		ABMReport report = ctx.getBean(ABMReport.class);
		
		Configuration conf = report.getHBaseConf();		
		System.out.println("hbase configuration: " + conf);
		report.behaviourImpl.setConf(conf);
		report.behaviourImpl.run();
		
	}

}
