package org.hadoop.learning.ChainMapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Second mapper of the program
// LUPA 1,JOHN 1,STEVE 1,KUOA 1,LEXA 1
public class MyMapper2  extends Mapper<Text , IntWritable, Text,IntWritable>{

	public void map(Text key,IntWritable value, Context con) throws IOException, InterruptedException {
		
		String s1= key.toString().toLowerCase();
		con.write(new Text(s1), value);
	}
}
// output to reducer: lupa 1,john 1,steve 1,kuoa 1,lexa 1

