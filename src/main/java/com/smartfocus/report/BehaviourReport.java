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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.smartfocus.demo.ConsoleProgressBar;
import com.smartfocus.demo.DateEventCollector;
import com.smartfocus.demo.ParentHBaseDAO;

@Service
public class BehaviourReport extends ParentHBaseDAO {
	
	@Value("${hbase.database:IPS3StandAlone}")
	private String database;
	@Value("${report.group.by.date.format:'MMM/yyyy'}")
	private String groupByDateFormat;
	
	//private static final String INDEX_TABLE = ENV +  "BehaviourIndexes";
	private static final byte[] DATA_CF = toBytes("d");

	// PB Fields
//	private static final byte[] FIELD_JSON = toBytes("f_json");

	// Index + Bucket Qualifiers
//	private static final byte[] FIXED_BUCKET = toBytes((byte) 0x01);
//	private static final byte[] QUALIFIER_PRIMARY = toBytes('b');
//	private static final byte[] QUALIFIER_USER_REVERSE_BEHAVIOUR = toBytes('u');
//	private static final byte[] QUALIFIER_USER_TYPE_REVERSE_BEHAVIOUR = toBytes('v');
//	private static final byte[] QUALIFIER_ITEM_TYPE_BEHAVIOUR = toBytes('i');
//	private static final byte[] QUALIFIER_TYPE_BEHAVIOUR = toBytes('t');
//	private static final byte[] FIELD_BY_USER_REVERSE_BEHAVIOUR_INDEX_KEY = toBytes("ik_urb");
//	private static final byte[] FIELD_BY_USER_TYPE_REVERSE_BEHAVIOUR_INDEX_KEY = toBytes("ik_utrb");
//	private static final byte[] FIELD_BY_ITEM_TYPE_BEHAVIOUR_INDEX_KEY = toBytes("ik_itb");
	private static final byte[] FIELD_BY_TYPE_BEHAVIOUR_INDEX_KEY = toBytes("ik_tb");
		
	private Configuration conf;
	
	@Value("${participant.account.ids}")
	private String participantAccountIDs;
	
	@Value("${regex.behaviour.type.filter:'offer-open'}")
	private String regexBehaviourTypeFilter;
	
	@Value("${progress.bar.enabled: true}")
	private boolean progressBarEnabled;
	
	@Value("${progress.bar.refresh.interval.ms: 100}")	
	private int progresBarRefreshInterval;
	
	public BehaviourReport() {	
	}
	
	public BehaviourReport setConf(Configuration conf) {
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
		
		addIndexColumns(scan);
		//scan.addColumn(DATA_CF, FIELD_JSON);
		
		String hbaseTable = database + "_Behaviours";
		// Instantiating HTable class
		HTable table = new HTable(conf, hbaseTable);
		
		ValueFilter filter = new ValueFilter(CompareOp.EQUAL, new RegexStringComparator(regexBehaviourTypeFilter));
		scan.setFilter(filter);
		
		//scan.setMaxResultSize(1);
		//scan.setBatch(500);
		
		// Scanning the required columns
		ConsoleProgressBar progress = new ConsoleProgressBar(progresBarRefreshInterval);

		ResultScanner scanner = table.getScanner(scan);
		long count = 0;
		try {			
			progress.setMessage("Search done...processing results");
			DateEventCollector collector = new DateEventCollector(groupByDateFormat);			
			// Reading values from scan result
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				count++;
				//progress.setStatus(count);
				if(progressBarEnabled) {
					progress.setStatus(count, "Processing rows " + count);
				}				
				long timestamp = result.rawCells()[0].getTimestamp();
				collector.addEventTime(timestamp);
			}
			progress.setMessage("\nParticipantAccountID: " + paid);
			progress.setMessage("Table scanned " + hbaseTable);
			progress.setMessage("Filtered by \""+ regexBehaviourTypeFilter + "\"");
			for(Entry<String, AtomicLong> entry : collector.getDateCountMap().entrySet()) {
				System.out.println(entry.getKey() + "=" + entry.getValue());
			}					
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
	
//	private byte[] buildRowKey(long participantAccountID, long behaviourID) {
//		return joinBytes(toBytes((int) participantAccountID), QUALIFIER_PRIMARY, FIXED_BUCKET, toBytes(behaviourID));
//	}
	
	/**
     * Add columns to a Scan request to ensure the Result contains all index columns (in addition to
     * whatever else is required)
     * 
     * @param scan
     *            The Scan request
     */
	private void addIndexColumns(Scan scan) {
		//scan.addColumn(DATA_CF, FIELD_BY_USER_REVERSE_BEHAVIOUR_INDEX_KEY);
		//scan.addColumn(DATA_CF, FIELD_BY_USER_TYPE_REVERSE_BEHAVIOUR_INDEX_KEY);
		//scan.addColumn(DATA_CF, FIELD_BY_ITEM_TYPE_BEHAVIOUR_INDEX_KEY);
		scan.addColumn(DATA_CF, FIELD_BY_TYPE_BEHAVIOUR_INDEX_KEY);
	}
}
