package org.hadoop.learning.MultipleOutputClass;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		//Dataset input is present in git as mo.txt
		Configuration c = new Configuration();
		Path inputpath = new Path("hdfs://localhost:8020/user/resources/MultipleOutputClass_InputFile");
		Path outputdir= new Path("hdfs://localhost:8020/user/resources/MultipleOutputClass_OutputFile");
		Job j = Job.getInstance(c,"MyDriver");
		j.setMapperClass(MultiOutMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, inputpath);
		FileOutputFormat.setOutputPath(j, outputdir);
		
		//Set 2 output
		MultipleOutputs.addNamedOutput(j, "HR", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(j, "Accounts", TextOutputFormat.class, Text.class, Text.class);
		System.exit(j.waitForCompletion(true)?1:0);
	}
}
