package org.hadoop.learning.CustomWritableClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {
	
	public static void main (String [] args) throws Exception{
		
		Configuration c= new Configuration();
		Path inputpath= new Path("hdfs://localhost:8020/user/resources/CustomInputDataSet");
		Path outputpath= new Path("hdfs://localhost:8020/user/resources/CustomClassOutput");
		Job j = Job.getInstance(c,"MyDriver");
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(MyWritable.class);
		j.setMapOutputValueClass(IntWritable.class);
		j.setOutputKeyClass(MyWritable.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, inputpath);
		FileOutputFormat.setOutputPath(j, outputpath);
		System.exit(j.waitForCompletion(true)?1:0);
	}
}
