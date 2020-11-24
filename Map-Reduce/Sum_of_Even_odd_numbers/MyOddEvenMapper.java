package org.hadoop.learning;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyOddEvenMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	public void map(LongWritable key,Text value,Context con) throws IOException,InterruptedException{
		
		String s=value.toString();
		String [] list_of_values=s.split(",");
		for (String val : list_of_values) {
			int number=Integer.parseInt(val);
			if(number%2==1) {
				con.write(new Text("ODD"),new IntWritable(number));
			}
			else {
				con.write(new Text("EVEN"), new IntWritable(number));
			}
		}
	}

}
