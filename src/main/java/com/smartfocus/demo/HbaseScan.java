package com.smartfocus.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseScan {

	private Configuration conf;

	private HbaseScan() {
		// Instantiating Configuration class
		conf = HBaseConfiguration.create();
		//String hbaseHost = "172.26.68.83";
		//String zookeeperHost = "172.26.68.83";
		String hbaseHost = "localhost";
		String zookeeperHost = "localhost";
		
		// conf.setInt("timeout", 120000);
		conf.setInt("timeout", 10000);
		conf.set("hbase.master", hbaseHost + ":9000");
		conf.set("hbase.zookeeper.quorum", zookeeperHost);
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		System.out.println("Hbase conf:" + conf);

	}

	private void run() throws IOException {
		
		new BehaviourImpl().setConf(conf).run();
		
	}

	public static void main(String args[]) throws IOException {
		new HbaseScan().run();
	}
}
