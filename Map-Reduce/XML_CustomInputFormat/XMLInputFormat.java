package org.hadoop.learning.CustomInputFormatter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XMLInputFormat extends TextInputFormat{
	
	@Override
	public RecordReader<LongWritable,Text> createRecordReader(InputSplit split,TaskAttemptContext context) {
		
		// returns object of our record reader class having return type as RecordReader<LongWritable,Text>
		// In XMLRecordReader we are extending base class of RecordReader
		return new XMLRecordReader();
	}

}
