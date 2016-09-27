package com.smartfocus.demo;

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

@Service
public class BehaviourImpl extends ParentHBaseDAO {
	
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
//	private static final byte[] QUALIFIER_USER_REVERSE_BEHAVIOUR = toBytes('u');
//	private static final byte[] QUALIFIER_USER_TYPE_REVERSE_BEHAVIOUR = toBytes('v');
//	private static final byte[] QUALIFIER_ITEM_TYPE_BEHAVIOUR = toBytes('i');
//	private static final byte[] QUALIFIER_TYPE_BEHAVIOUR = toBytes('t');
//	private static final byte[] FIELD_BY_USER_REVERSE_BEHAVIOUR_INDEX_KEY = toBytes("ik_urb");
//	private static final byte[] FIELD_BY_USER_TYPE_REVERSE_BEHAVIOUR_INDEX_KEY = toBytes("ik_utrb");
//	private static final byte[] FIELD_BY_ITEM_TYPE_BEHAVIOUR_INDEX_KEY = toBytes("ik_itb");
	private static final byte[] FIELD_BY_TYPE_BEHAVIOUR_INDEX_KEY = toBytes("ik_tb");
		
	private Configuration conf;
	
	@Value("${participant.account.id:40001}")
	private int participantAccountID;
	
	@Value("${regex.behaviour.type.filter:'offer-open'}")
	private String regexBehaviourTypeFilter;
	
	public BehaviourImpl() {	
	}
	
	public BehaviourImpl setConf(Configuration conf) {
		this.conf = conf;
		return this;
	}
	
	public void run() throws IOException {
		long start = System.currentTimeMillis();
		
		// Instantiating the Scan class
		Scan scan = new Scan();
		scan.addFamily(DATA_CF);
		scan.setStartRow(toBytes((int) participantAccountID));
		scan.setStopRow(toBytes((int) participantAccountID + 1));
		
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
		ConsoleProgressBar progress = new ConsoleProgressBar("Scanning...");

		ResultScanner scanner = table.getScanner(scan);
		try {
			long searchDone = System.currentTimeMillis() - start;
			long count = 0;
			System.out.println("Search done...processing results");
			DateEventCollector collector = new DateEventCollector(groupByDateFormat);
			
			// Reading values from scan result
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				count++;
				//progress.setStatus(count);
				progress.setMessage("Processing rows " + count);
				
				long timestamp = result.rawCells()[0].getTimestamp();
				collector.addEventTime(timestamp);
				//System.out.println("\n" + timestamp + ", " + new Date(timestamp) + ", offer-open");
				//String s = Bytes.toString(result.getValue(DATA_CF,  FIELD_JSON));
				//System.out.println(Bytes.toString(result.getValue(DATA_CF,  Bytes.toBytes("f_json"))));
				//System.out.println(Bytes.toString(result.getValue(DATA_CF,  QUALIFIER_TYPE_BEHAVIOUR)));
	//			for (byte[] rowKey : result.getFamilyMap(DATA_CF).keySet()) {
	//				System.out.print("\trowkey? " + Bytes.toString(rowKey));
	//			}
				
	//			System.out.println(
	//					 Bytes.toString(
	//					result.getValue(DATA_CF, Bytes.toBytes("ik_tb"))));
				// System.out.println("Found row : " + result);
//				try {
//					Thread.sleep(20L);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
			System.out.println("\nParticipantAccountID: " + participantAccountID);
			System.out.println("Table scanned " + hbaseTable);
			System.out.println("Filtered by \""+ regexBehaviourTypeFilter + "\"");
			for(Entry<String, AtomicLong> entry : collector.getDateCountMap().entrySet()) {
				System.out.println(entry.getKey() + "=" + entry.getValue());
			}		
			System.out.println("Search completed in " + searchDone + "ms with " + count + " rows found");
			//System.out.println("scanner closed");
			System.out.println("The whole process completed in " + (System.currentTimeMillis() - start) + "ms");
		} finally {
			table.close();
			// closing the scanner
			scanner.close();
		}
	}
	
	private byte[] buildRowKey(long participantAccountID, long behaviourID) {
		return joinBytes(toBytes((int) participantAccountID), QUALIFIER_PRIMARY, FIXED_BUCKET, toBytes(behaviourID));
	}
	
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
