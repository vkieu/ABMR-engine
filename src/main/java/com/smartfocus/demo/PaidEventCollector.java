package com.smartfocus.demo;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class PaidEventCollector {
		
	private Map<String, AtomicLong> countMap;
	
	public PaidEventCollector() {
		this.countMap = new TreeMap<>();
	}
	
	public void addEvent(String paid) {		
		AtomicLong count = countMap.get(paid);
		if(count == null) {
			count = new AtomicLong(0);
			countMap.put(paid, count);
		}
		count.incrementAndGet();
	}

	public Map<String, AtomicLong> getCountMap() {
		return countMap;
	}
	

}
