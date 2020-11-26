package org.hadoop.learning.MultipleInputClass;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper2 extends Mapper<Text,Text,Text,IntWritable>{
	
	@Override
	public void map(Text key, Text value,Context con) throws IOException, InterruptedException{
	
		con.write(value, new IntWritable(1));
	}
}
