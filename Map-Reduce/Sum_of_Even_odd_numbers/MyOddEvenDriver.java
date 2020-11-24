package org.hadoop.learning;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class MyOddEvenDriver {

	public static void main (String [] args) throws Exception{
		
		Configuration c =new Configuration();
		String [] files= new GenericOptionsParser(c,args).getRemainingArgs();
		Path input= new Path("hdfs://localhost:8020/user/resources/evenoddinput");
		Path output= new Path("hdfs://localhost:8020/user/resources/evenoddOutput");
		Job j = Job.getInstance(c,"MyOddEvenDriver");
		j.setJarByClass(MyOddEvenDriver.class);
		j.setMapperClass(MyOddEvenMapper.class);
		j.setReducerClass(MyOddEvenReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?1:0);
	}
}
