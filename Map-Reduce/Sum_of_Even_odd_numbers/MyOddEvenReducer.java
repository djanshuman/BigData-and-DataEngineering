package org.hadoop.learning;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyOddEvenReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	public void reduce(Text key,Iterable<IntWritable> values, Context con) throws IOException,InterruptedException{
		
		int sum=0;
		if(key.equals("ODD")) {
			for (IntWritable val : values) {
				sum+=val.get();
			}
		}
		else {
			for (IntWritable val : values) {
				sum+=val.get();
			}
		}
		con.write(key,new IntWritable(sum));
	}

}
