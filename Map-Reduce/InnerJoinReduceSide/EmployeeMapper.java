package org.hadoop.learning.InnerJoinReduceSide;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//1 JACK

public class EmployeeMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key, Text value, Context con) throws IOException,InterruptedException{
		String s= value.toString().trim();
		String [] list_of_values=s.split(" ");
		con.write(new Text(new Text(list_of_values[0])), new Text("EMPLOYEE" +","+list_of_values[1]));
	}

}

// 1 , EMPLOYEE,JACK
