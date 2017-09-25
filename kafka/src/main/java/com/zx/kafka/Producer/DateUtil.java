package com.zx.kafka.Producer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;


public class DateUtil {
	
	public static String getTime3(Long time) {
        Date dat=new Date(time);  
        GregorianCalendar gc = new GregorianCalendar();   
        gc.setTime(dat);  
        java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-M-d HH:mm:ss");  
        String sb=format.format(gc.getTime()); 
        return sb;
	}
	
	public static void main(String[] args) {
		
		System.out.println(display("2016-12-24","2017-08-20").size());
		
	}
	
    public static List<String> display(String dateFirst, String dateSecond){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        List<String> everyday = new ArrayList<String>(); 
        try{
            Date dateOne = dateFormat.parse(dateFirst);
            Date dateTwo = dateFormat.parse(dateSecond);
             
            Calendar calendar = Calendar.getInstance();
             
            calendar.setTime(dateOne);
             
            while(calendar.getTime().before(dateTwo)){               
                everyday.add(dateFormat.format(calendar.getTime()));
                calendar.add(Calendar.DAY_OF_MONTH, 1);               
            }
            return everyday;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

}
