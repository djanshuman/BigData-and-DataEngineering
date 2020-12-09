package org.hadoop.learning.BankLoyalCustomer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//OMOI808692OZ,1245015582,savings,3667,822,no
public class AccountMapper extends Mapper<LongWritable,Text,Text,Text> {
	
	@Override
	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {
	
		String []account_details= value.toString().split(",");
		con.write(new Text(account_details[0]), new Text("A,"+account_details[1] +","+account_details[3]+","+account_details[4]+","+account_details[5]));
		//OMOI808692OZ A,1245015582,3667,822,no
	}
}
