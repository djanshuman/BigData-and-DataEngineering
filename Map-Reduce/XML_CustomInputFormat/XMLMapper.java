package org.hadoop.learning.CustomInputFormatter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//identity mapper runs the input key,value as is and no transformation.
public class XMLMapper extends Mapper<LongWritable,Text,LongWritable,Text >{
	
	@Override
	protected void map(LongWritable key, Text value,Context con) throws IOException, InterruptedException
	{
		con.write(key, value);
	}

}
