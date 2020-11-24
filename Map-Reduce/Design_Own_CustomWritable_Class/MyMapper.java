package org.hadoop.learning.CustomWritableClass;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable,Text,MyWritable,IntWritable>{
	
	public void map(LongWritable key,Text value,Context con) throws IOException,InterruptedException{
		
		String s= value.toString();
		String [] s1= s.split(",");
		for (String word : s1) {
			MyWritable keyoutput= new MyWritable(word);
			IntWritable valueoutput=new IntWritable(1);
			con.write(keyoutput,valueoutput);
		}
	}
}
