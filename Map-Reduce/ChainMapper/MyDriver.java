package org.hadoop.learning.ChainMapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyDriver {
	
	public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		Configuration c= new Configuration();
		Path inputpath= new Path("hdfs://localhost:8020/user/resources/wordCountInput/word_count.txt");
		Path outputpath= new Path("hdfs://localhost:8020/user/resources/wordCountOutput");
		Job j = Job.getInstance(c,"MyDriver");
		
    //For ChainMapper implementation we are using ChainMapper and ChainReducer class.
		//Mapper1
		ChainMapper.addMapper(
				j, 
				MyMapper1.class, 
				LongWritable.class, 
				Text.class, 
				Text.class, 
				IntWritable.class, 
				c);
		//Mapper2
		ChainMapper.addMapper(
				j, 
				MyMapper2.class, 
				Text.class, 
				IntWritable.class, 
				Text.class, 
				IntWritable.class, 
				c);
		//Reducer
		ChainReducer.setReducer(
				j, 
				MyReducer.class, 
				Text.class, 
				IntWritable.class, 
				Text.class, 
				IntWritable.class, 
				c);
		FileInputFormat.addInputPath(j, inputpath);
		FileOutputFormat.setOutputPath(j, outputpath);
		System.exit(j.waitForCompletion(true)?1:0);
		
	}

}
