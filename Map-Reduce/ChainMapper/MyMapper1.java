package org.hadoop.learning.ChainMapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//First Mapper of the program.
// Input: LUPA,JOHN,STEVE,KUOA,LEXA

public class MyMapper1 extends Mapper<LongWritable , Text, Text,IntWritable>{
	@Override
	public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException {
		
		String []s1=value.toString().split(",");
		for (String s: s1) {
			con.write(new Text(s), new IntWritable(1));
		}
	}

}
