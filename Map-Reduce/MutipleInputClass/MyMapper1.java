package org.hadoop.learning.MultipleInputClass;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//mapper1 to read text file with space separated.
public class MyMapper1 extends Mapper <LongWritable,Text,Text,IntWritable>{
	
	
	@Override
	public void map(LongWritable key,Text value,Context con) throws IOException,InterruptedException{
		
		IntWritable count = new IntWritable(1);
		String word=value.toString();
		String [] list_of_words=word.split(" ");
		for (String wrd : list_of_words) {
			con.write(new Text(wrd), count);
		}
	}

}
