package org.hadoop.learning.FraudCustomersDetection;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//GGYZ333519YS,Allison,08-01-2017,10-01-2017,Delhivery,13-01-2017,yes,15-01-2017,Damaged Item

public class MyMapper extends Mapper<LongWritable,Text,Text,MyFraudWritable>{
	
	private Text custid=new Text();
	private MyFraudWritable orderDetails= new MyFraudWritable();
	public void map(LongWritable key,Text value,Context con) throws IOException,java.lang.InterruptedException{
		
		String f=value.toString();
		String [] list_of_values=f.split(",");
		custid.set(list_of_values[0]);
		orderDetails.set(list_of_values[1], list_of_values[5], list_of_values[6], list_of_values[7]);
		con.write(custid, orderDetails);
	}
}

//GGYZ333519YS, Allison,13-01-2017,yes,15-01-2017
