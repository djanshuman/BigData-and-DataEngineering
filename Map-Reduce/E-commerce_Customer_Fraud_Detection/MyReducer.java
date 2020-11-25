package org.hadoop.learning.FraudCustomersDetection;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//GGYZ333519YS, Allison,13-01-2017,yes,15-01-2017


public class MyReducer extends Reducer<Text,MyFraudWritable,Text,IntWritable> {

	ArrayList<String> customers= new ArrayList<String>();
	public void reduce(Text key, Iterable<MyFraudWritable> values,Context con) throws IOException,InterruptedException{
		
		int fraudpoints=0;
		int orderCount=0;
		int returnCount=0;
		MyFraudWritable list_of_values = null;
		
		Iterator<MyFraudWritable> itr= values.iterator();
		
		while (itr.hasNext()) {
			
			orderCount++;
			list_of_values= itr.next();
			if(list_of_values.getReturned()) {
				
				returnCount++;
				try {
					
					SimpleDateFormat sdf= new SimpleDateFormat("dd-MM-yyyy");
					Date receiveDate=sdf.parse(list_of_values.getReceivedDate());
					Date returnedDate=sdf.parse(list_of_values.getReturnedDate());
					
					long DiffinMillis=Math.abs(returnedDate.getTime()-receiveDate.getTime());
					long DiffinDays=TimeUnit.DAYS.convert(DiffinMillis,TimeUnit.MILLISECONDS);
					
					//1 Fraudpoint to the customer whose returnDate - receiveDate is > 10days
					if(DiffinDays > 10) {
						fraudpoints++;
					}
					
				   }
				catch(Exception e){
					e.printStackTrace();
				}
			}
		}
		/* 10 fraud points to the customer whose return rate is more than 50% */
		double returnrate=(returnCount/(orderCount*1.0))*100;
		if(returnrate >= 50) {
			fraudpoints+=10;
		}
		
		customers.add(key +","+list_of_values.getCustomername()+","+fraudpoints);
		//GGYZ333519YS,Allison,12
	}
	public void cleanup(Context con) throws IOException, InterruptedException{
		
		Collections.sort(customers,new Comparator<String>()
		{
			public int compare(String s1, String s2) {
				int fp1= Integer.parseInt(s1.split(",")[2]);
				int fp2= Integer.parseInt(s2.split(",")[2]);
				return -(fp1-fp2);
				
		}});
		
		for (String f : customers) {
			String [] words = f.split(",");
			con.write(new Text(words[0] +","+words[1]), new IntWritable(Integer.parseInt(words[2])));
		}
	}
}
