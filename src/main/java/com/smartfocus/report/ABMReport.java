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

@ComponentScan("com.smartfocus.report")
@PropertySource(value="file:global.properties")
@SpringBootApplication
public class ABMReport {

	@Value("${hbase.host.url:localhost}")
	private String hbaseHostUrl;
	@Value("${hbase.zookeeper.quorumPeer:localhost}")
	private String zookeeperQuorumPeer;
	@Value("${hbase.zookeeper.port:2181}")
	private int zookeeperPort;	
	@Value("${hbase.connection.timeout:12000}")
	private int timeout;
	
	@Value("${report.multi.paid.search:false}")
	private boolean reportPaidSearch;
	
	@Value("${report.single.paid.search:false}")
	private boolean reportSinglePaidSearch;
	
	@Autowired
	private BehaviourReport behaviourReport;
	@Autowired
	private BehaviourPaidReport behaviourPaidReport;
	
	private Configuration getHBaseConf() {
		Configuration conf = HBaseConfiguration.create();				
		conf.setInt("timeout", timeout);
		conf.set("hbase.master", hbaseHostUrl);
		conf.set("hbase.zookeeper.quorum", zookeeperQuorumPeer);
		conf.setInt("hbase.zookeeper.property.clientPort", zookeeperPort);
		return conf;
	}
		
	public static void main(String[] args) throws Exception {
		
		ApplicationContext ctx = SpringApplication.run(ABMReport.class, args);		
		ABMReport report = ctx.getBean(ABMReport.class);
		
		Configuration conf = report.getHBaseConf();		
		System.out.println("hbase configuration: " + conf);
		
		if(report.reportPaidSearch) {
			report.behaviourPaidReport.setConf(conf);
			report.behaviourPaidReport.run();
		}
		
		if(report.reportSinglePaidSearch) {
			report.behaviourReport.setConf(conf);
			report.behaviourReport.run();
		}
				
		
	}

}
