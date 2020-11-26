package org.hadoop.learning.MultipleInputClass;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {
	
	public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Path inputpath1 = new Path("hdfs://localhost:8020/user/resources/MutipleInputClassInput1");
		Path inputpath2 = new Path("hdfs://localhost:8020/user/resources/MutipleInputClassInput2");
		Path outputpath = new Path("hdfs://localhost:8020/user/resources/MutipleInputClassOutput");
		
		Configuration c = new Configuration();
		//below line add the key value separator to comma. Default is tab separated.
		c.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		Job j = Job.getInstance(c,"MyDriver");
		j.setMapperClass(MyMapper1.class);
		j.setMapperClass(MyMapper2.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(IntWritable.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		//MultipleInput class required for identifying and merging during job/task execution
		MultipleInputs.addInputPath(j, inputpath1, TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(j, inputpath2, KeyValueTextInputFormat.class,MyMapper2.class);
		
		FileOutputFormat.setOutputPath(j, outputpath);
		System.exit(j.waitForCompletion(true)?1:0);
		
	}

}
