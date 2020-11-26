package org.hadoop.learning.MultipleInputClass;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Frank , [1,1,1,1]
public class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	
	public void reduce(Text key,Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
		int sum=0;
		for (IntWritable count :values ) {
			sum+=count.get();
	}
		con.write(key, new IntWritable(sum));
		
	}
}
