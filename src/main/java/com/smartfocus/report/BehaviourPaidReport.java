package com.smartfocus.report;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.smartfocus.demo.ConsoleProgressBar;
import com.smartfocus.demo.PaidEventCollector;
import com.smartfocus.demo.ParentHBaseDAO;

@Service
public class BehaviourPaidReport extends ParentHBaseDAO {
	
	@Value("${hbase.database:IPS3StandAlone}")
	private String database;
	@Value("${report.group.by.date.format:'MMM/yyyy'}")
	private String groupByDateFormat;
	
	//private static final String INDEX_TABLE = ENV +  "BehaviourIndexes";
	private static final byte[] DATA_CF = toBytes("d");

	// PB Fields
	private static final byte[] FIELD_JSON = toBytes("f_json");

	// Index + Bucket Qualifiers
	private static final byte[] FIXED_BUCKET = toBytes((byte) 0x01);
	private static final byte[] QUALIFIER_PRIMARY = toBytes('b');
	private static final byte[] FIELD_BY_TYPE_BEHAVIOUR_INDEX_KEY = toBytes("ik_tb");
		
	private Configuration conf;
		
	@Value("${progress.bar.enabled: true}")
	private boolean progressBarEnabled;
	
	@Value("${progress.bar.refresh.interval.ms: 100}")	
	private int progresBarRefreshInterval;
	
	public BehaviourPaidReport() {	
	}
	
	public BehaviourPaidReport setConf(Configuration conf) {
		this.conf = conf;
		return this;
	}
	
	public void run() throws IOException {
		long start = System.currentTimeMillis();
		
		// Instantiating the Scan class
		Scan scan = new Scan();
		scan.addFamily(DATA_CF);
		scan.addColumn(DATA_CF, FIELD_JSON);
		
		String hbaseTable = database + "_Behaviours";
		// Instantiating HTable class
		HTable table = new HTable(conf, hbaseTable);
				
		//scan.setMaxResultSize(1);
		//scan.setBatch(500);
		
		// Scanning the required columns
		ConsoleProgressBar progress = new ConsoleProgressBar(progresBarRefreshInterval);

		ResultScanner scanner = table.getScanner(scan);
		try {
			long count = 0;
			progress.setMessage("Search done...processing results");
			PaidEventCollector collector = new PaidEventCollector();
			
			// Reading values from scan result
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				count++;
				//progress.setStatus(count);
				if(progressBarEnabled) {
					progress.setStatus(count, "Processing rows " + count);
				}
				
				//long timestamp = result.rawCells()[0].getTimestamp();
				JSONObject object = new JSONObject(new String(result.getValue(DATA_CF, FIELD_JSON)));
				String paid = object.getString("paid");
				collector.addEvent(paid);
				
			}
			progress.setMessage("Table scanned " + hbaseTable);
			for(Entry<String, AtomicLong> entry : collector.getCountMap().entrySet()) {
				System.out.println(entry.getKey() + "=" + entry.getValue());
			}		
			//progress.setMessage("Search completed in " + searchDone + "ms with " + count + " rows found");
			//System.out.println("scanner closed");
			progress.setMessage("The whole process completed in " + (System.currentTimeMillis() - start) + "ms");
			
		} catch(JSONException e) {
			throw new RuntimeException(e);
		} finally {
			table.close();
			// closing the scanner
			scanner.close();
		}
	}
	
//	private byte[] buildRowKey(long participantAccountID, long behaviourID) {
//		return joinBytes(toBytes((int) participantAccountID), QUALIFIER_PRIMARY, FIXED_BUCKET, toBytes(behaviourID));
//	}
		
}
