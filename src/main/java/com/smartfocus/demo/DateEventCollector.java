package com.smartfocus.demo;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class DateEventCollector {
	
	private String groupByDateFormat = "MMM/yyyy";	
	private DateFormat df;
	private Map<String, AtomicLong> dateCountMap;
	
	public DateEventCollector(String groupByDateFormat) {
		this.groupByDateFormat = groupByDateFormat;
		this.df = new SimpleDateFormat(this.groupByDateFormat);
		this.dateCountMap = new TreeMap(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				try {					
					Date od1 = df.parse(o1);
					Date od2 = df.parse(o2);
					int result =  od1.compareTo(od2);
					return result;
				} catch (ParseException e) {
					//should never occurred
					throw new RuntimeException(e);
				}
			}
		});
	}
	
	public void addEventTime(long ms) {
		String dateString = df.format(new Date(ms));
		AtomicLong count = dateCountMap.get(dateString);
		if(count == null) {
			count = new AtomicLong(0);
			dateCountMap.put(dateString, count);
		}
		count.incrementAndGet();
	}

	public Map<String, AtomicLong> getDateCountMap() {
		return dateCountMap;
	}
	

}
