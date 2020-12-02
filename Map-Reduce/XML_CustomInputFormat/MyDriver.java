package org.hadoop.learning.CustomInputFormatter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {
	
	public static void main(String [] args) throws IOException,InterruptedException, ClassNotFoundException{
		
		Configuration c= new Configuration();
		Path inputpath= new Path("hdfs://localhost:8020/user/resources/CustomInputFormatDataSet/xml.txt");
		Path outputpath= new Path("hdfs://localhost:8020/user/resources/CustomOutputFormatOutputFile");
		Job j = Job.getInstance(c,"MyDriver");
		
		//adding our own Custom Input Format class 
		j.setInputFormatClass(XMLInputFormat.class);
		j.setMapperClass(XMLMapper.class);
		j.setJarByClass(MyDriver.class);
		
		j.setOutputKeyClass(LongWritable.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, inputpath);
		FileOutputFormat.setOutputPath(j, outputpath);
		System.exit(j.waitForCompletion(true)?1:0);
	}

}
