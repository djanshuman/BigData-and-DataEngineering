package org.hadoop.learning.CustomCounters;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {

	public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Path Inputpath = new Path("hdfs://localhost:8020/user/resources/CustomCounterInput/counter.txt");
		Path Outputpath= new Path("hdfs://localhost:8020/user/resources/CustomCounterOutput/");
		Configuration c = new Configuration();
		Job j = Job.getInstance(c);
		j.setJarByClass(MyDriver.class);
		j.setMapperClass(CustomCounterMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j,Inputpath);
		FileOutputFormat.setOutputPath(j,Outputpath);
		System.exit(j.waitForCompletion(true)?1:0);
		
	}
}
