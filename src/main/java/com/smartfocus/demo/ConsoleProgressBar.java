package com.smartfocus.demo;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ConsoleProgressBar {
	
	public static void main(String[] argv) throws Exception{
        String anim= "|/-\\";
        for (int x =0 ; x <= 100 ; x++){
                String data = "\r" + anim.charAt(x % anim.length())  + " " + x ;
                System.out.write(data.getBytes());
                Thread.sleep(100);
        }
	}	
	
	private String message = "";
	private long status = 0;
	
	private final int refreshInterval;
	private long lastPrinted = 0;
	private int statusRoller = 0;
	private long started = 0;
	private final  DateFormat df;
	
	public ConsoleProgressBar (int refreshInterval) {
		df = new SimpleDateFormat("HH:mm:ss");
		this.refreshInterval = refreshInterval;
		this.started = System.currentTimeMillis();
	}
	
	public void setMessage(String message) {
		this.message = message;
		System.out.println(this.message);
	}
	
	public void setStatus(long status, String message) {
		this.status = status;
		this.message = message;
		printProgress();
	}
	
	private void printProgress() {
		if(System.currentTimeMillis() - lastPrinted > refreshInterval) {
			String anim= "|/-\\";
			String data = "\r" + anim.charAt(statusRoller++ % anim.length())  
					+ " elasped: "+ (df.format(new Date(System.currentTimeMillis() - started))) 
					+ " " + message ;
	        try {
				System.out.write(data.getBytes());
				lastPrinted = System.currentTimeMillis();
			} catch (IOException e) {
				//so what if we can't write to the console??? not possible
				System.out.println(e.getMessage());
			}
		}
	}
}
