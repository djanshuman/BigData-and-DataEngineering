package org.hadoop.learning.MultipleOutputClass;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//DJPX255251,Arthur,HR,6597,2017

public class MultiOutMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException{
		
		String s= value.toString();
		String[] list_of_values=s.split(",");
		String emp_id=list_of_values[0];
		String rest_values=list_of_values[1] +","+list_of_values[2]+","+list_of_values[3];
		con.write(new Text(emp_id),new Text(rest_values));
		
	}
//{DJPX255251, Arthur,HR,6597}

}
