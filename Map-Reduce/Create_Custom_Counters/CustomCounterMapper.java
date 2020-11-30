package org.hadoop.learning.CustomCounters;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Static counter is declared using enum. Location is group name and inside we have name of counters.
enum Location{
	TOTAL,BANGALORE,CHENNAI,HYDERABAD
}
//1194208577,sofa,39,3,Hyderabad

public class CustomCounterMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException{
		
		double sales_for_location=0;
		//Custom Counter to calculate total number of records processed by mapper
		con.getCounter(Location.TOTAL).increment(1);
		String val=value.toString();
		String [] list_of_values=val.split(",");
		
		//Custom Counter to calculate total number of records processed for each location
		if(list_of_values[4].equalsIgnoreCase("BANGALORE")) {
			con.getCounter(Location.BANGALORE).increment(1);
		}
		else if (list_of_values[4].equalsIgnoreCase("CHENNAI")) {
			con.getCounter(Location.CHENNAI).increment(1);
		}
		else if (list_of_values[4].equalsIgnoreCase("HYDERABAD")) {
			con.getCounter(Location.HYDERABAD).increment(1);
		}
		else {
			System.out.println("Invalid location");
		}
		
		// Dynamic counter Implementation. Sales is group name and low_sales/high_sales are counter variables
		if(Integer.parseInt(list_of_values[3]) < 10) {
			con.getCounter("SALES","LOW_SALES").increment(1);
		}
		sales_for_location=Double.parseDouble(list_of_values[2])*Double.parseDouble(list_of_values[3]);
		
		if(sales_for_location > 500) {
			con.getCounter("SALES","HIGH_SALES").increment(1);
		}
		
		Text store_location=new Text(list_of_values[4]);
		// Hyderabad 39,3
		con.write(store_location,new Text(list_of_values[2]+ ","+list_of_values[3]));
	}
}
