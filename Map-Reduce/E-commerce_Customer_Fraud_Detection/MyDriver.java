package org.hadoop.learning.FraudCustomersDetection;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {
	
	public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration c= new Configuration();
		Path input=new Path("hdfs://localhost:8020/user/resources/FraudCustomerInput");
		Path output= new Path("hdfs://localhost:8020/user/resources/FraudCustomerOutput");
		Job j =Job.getInstance(c,"MyDriver");
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(MyFraudWritable.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j,output);
		System.exit(j.waitForCompletion(true)?1:0);	
	}

}

