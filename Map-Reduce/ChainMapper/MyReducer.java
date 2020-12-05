package org.hadoop.learning.ChainMapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// program aggregates the final word count on the input received from mapper2
public class MyReducer extends Reducer<Text , IntWritable,Text,IntWritable>{

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
		
		int wordCount=0;
		for (IntWritable v1 :values) {
			wordCount+=v1.get();
		}
		con.write(key, new IntWritable(wordCount));
		
	}
	
}
