package com.smartfocus.demo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvCollector {
		
	private String[] headers;
	private final List<Map<String, Object>> objectList = new ArrayList<>();
	private final File outputFile;
	private static int FLUSH_SIZE = 500;
	private boolean outputHeader = false;
	
	public CsvCollector(String outFilePath) {
		outputFile = new File(outFilePath);
	}
	
	public void setHeaders(String[] headers) {
		if(headers == null || headers.length == 0) {
			throw new IllegalArgumentException("output headers MUST not be NULL");
		}
		this.headers = headers;
	}
	
	public void addObjectMap(Map<String, Object> map) throws IOException {
		if(this.headers == null) {
			this.headers = map.keySet().toArray(new String[0]);
		}
		objectList.add(map);
		if(objectList.size() > FLUSH_SIZE) {
			flush();
		}
	}
	
	public void flush() throws IOException {
		if(objectList.isEmpty()) {
			return;
		}
		if(headers == null || headers.length == 0) {
			throw new IllegalArgumentException("output headers MUST not be NULL");
		}
		if (!outputHeader) {
			writeCsvLine(headers);
		}		
		for(Map<String, Object> json : objectList) {
			ArrayList<String> values = new ArrayList<>();			
			for(String header: headers) {
				Object o = json.get(header);
				if(null == o) {
					values.add("");
				} else {
					values.add(String.valueOf(o));
				}
			}
			writeCsvLine(values.toArray(new String[0]));
		}
		objectList.clear();//flush done
	}
	
	private void writeCsvLine(String[] csvStrings) throws IOException {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < csvStrings.length; i++) {
			if(i > 0) {
				sb.append(",");
			}
			if(csvStrings[i].contains(",")) {
				sb.append("\"").append(csvStrings[i]).append("\"");
			} else {
				sb.append(csvStrings[i]);
			}			
		}
		FileWriter fw = new FileWriter(outputFile, true);
		fw.write(sb.toString() + "\n");
		fw.flush();
		fw.close();
	}
}
