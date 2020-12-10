package org.hadoop.learning.ChurnCustomerDetection;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//JXJY167254JK,21-06-2017,5589654,Quiche:Crispy Onion Rings:Roti,87,Card,Johnahs,08:31:21,2,Late delivery

public class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	@Override
	protected void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException{
		
		String [] order_details=value.toString().split(",");
		con.write(new Text(order_details[0]), new Text(order_details[1] +","+order_details[8]+","+order_details[9]));
	}

}
//JXJY167254JK 21-06-2017,2,Late delivery
