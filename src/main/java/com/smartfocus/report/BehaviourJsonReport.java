package com.smartfocus.report;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.mortbay.util.ajax.JSON;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.smartfocus.demo.ConsoleProgressBar;
import com.smartfocus.demo.CsvCollector;
import com.smartfocus.demo.ParentHBaseDAO;

@Service
public class BehaviourJsonReport extends ParentHBaseDAO {
	
	@Value("${hbase.database:IPS3StandAlone}")
	private String database;
	
	//private static final String INDEX_TABLE = ENV +  "BehaviourIndexes";
	private static final byte[] DATA_CF = toBytes("d");

	// PB Fields
	private static final byte[] FIELD_JSON = toBytes("f_json");
		
	private Configuration conf;
	
	@Value("${paid.json.export.ids}")
	private String participantAccountIDs;
		
	@Value("${progress.bar.enabled: true}")
	private boolean progressBarEnabled;
	
	@Value("${progress.bar.refresh.interval.ms: 100}")	
	private int progresBarRefreshInterval;
	
	public BehaviourJsonReport() {	
	}
	
	public BehaviourJsonReport setConf(Configuration conf) {
		this.conf = conf;
		return this;
	}
	
	private void searchPaid(final long paid) throws IOException {
		long start = System.currentTimeMillis();
		
		// Instantiating the Scan class
		Scan scan = new Scan();
		scan.addFamily(DATA_CF);
		scan.setStartRow(toBytes((int) paid));
		scan.setStopRow(toBytes((int) paid + 1));
		
		scan.addColumn(DATA_CF, FIELD_JSON);
		
		String hbaseTable = database + "_Behaviours";
		// Instantiating HTable class
		HTable table = new HTable(conf, hbaseTable);
				
		// Scanning the required columns
		ConsoleProgressBar progress = new ConsoleProgressBar(progresBarRefreshInterval);

		ResultScanner scanner = table.getScanner(scan);
		long count = 0;
		try {			
			progress.setMessage("Search done...processing results");
			CsvCollector collector = new CsvCollector(paid + "_" + start + ".csv");
			
			// Reading values from scan result
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				count++;
				//progress.setStatus(count);
				if(progressBarEnabled) {
					progress.setStatus(count, "Processing rows " + count);
				}				
				String jsonString = new String(result.getValue(DATA_CF, FIELD_JSON));
				//System.out.println(jsonString);
				
				Map<String, Object>	objectMap = (Map<String, Object>)JSON.parse(new StringReader(jsonString));
				collector.addObjectMap(objectMap);
			}
			collector.flush();
			progress.setMessage("\nParticipantAccountID: " + paid);
			progress.setMessage("Table scanned " + hbaseTable);
								
		} finally {
			table.close();
			// closing the scanner
			scanner.close();
		}		
		progress.setMessage("search PAID: " + paid + " completed in " + (System.currentTimeMillis() - start) + "ms");
	}
	
	public void run() throws IOException {
		String[] paids = participantAccountIDs.split(",");
		for(int i = 0; i < paids.length; i++ ) {
			searchPaid(Long.valueOf(paids[i].trim()));
		}		
	}
	

}
