package com.smartfocus.demo;

import java.io.IOException;

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
	
	private static final int REFRESH_PERIOD = 100;
	private static long lastPrinted = 0;
	private int statusRoller = 0;
	
	public ConsoleProgressBar (String message) {
		this.message = message;
	}
	
	public void setMessage(String message) {
		this.message = message;
		output();
	}
	
	public void setStatus(long status) {
		this.status = status;
		output();
	}
	public void output() {
		if(System.currentTimeMillis() - lastPrinted > REFRESH_PERIOD) {
			String anim= "|/-\\";
			String data = "\r" + anim.charAt(statusRoller++ % anim.length())  + " " + status + " " + message ;
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
