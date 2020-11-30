package org.hadoop.learning.CustomCounters;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Hyderabad,[{39,3} ,{54,13} , {48,15}]
public class MyReducer extends Reducer<Text,Text,Text,IntWritable> {
	
	public void reduce(Text key, Iterable<Text>values, Context con) throws IOException, InterruptedException {
		int total_revenue=0;
		Iterator<Text> val=values.iterator();
		while (val.hasNext()) 
		{
			String fetch_value=val.next().toString();
			String [] list_of_values=fetch_value.split(",");
			total_revenue+=Integer.parseInt(list_of_values[0]) * Integer.parseInt(list_of_values[1]);
		}
		con.write(key, new IntWritable(total_revenue)); // Hyderabad	18781
	}
}
