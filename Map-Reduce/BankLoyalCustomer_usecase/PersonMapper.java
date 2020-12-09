package org.hadoop.learning.BankLoyalCustomer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//OMOI808692OZ,Allison,Abbott,21,female,Chicago

public class PersonMapper extends Mapper<LongWritable,Text,Text,Text>{
	@Override
	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {
		
		String[] person_details=value.toString().split(",");
		con.write(new Text(person_details[0]), new Text("P,"+person_details[1]+","+person_details[2]));
		//OMOI808692OZ P,Allison,Abbott
	}
}
