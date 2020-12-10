package org.hadoop.learning.ChurnCustomerDetection;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {
	
	public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration c= new Configuration();
		Path input=new Path("hdfs://localhost:8020/user/resources/ChurnCustomerInput/food.txt");
		Path output= new Path("hdfs://localhost:8020/user/resources/ChurnCustomerOutput");
		Job j =Job.getInstance(c,"MyDriver");
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j,output);
		System.exit(j.waitForCompletion(true)?1:0);	
	}

}

