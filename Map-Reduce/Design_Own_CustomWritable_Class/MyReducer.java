package org.hadoop.learning.CustomWritableClass;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<MyWritable,IntWritable,MyWritable,IntWritable>{
	
	public void reduce(MyWritable key,Iterable<IntWritable> values,Context con) throws IOException,InterruptedException{
		
		int sum=0;
		Iterator <IntWritable> itr=values.iterator();
		while(itr.hasNext()) {
			sum+=itr.next().get();
		}
		con.write(key, new IntWritable(sum));
	}

}
