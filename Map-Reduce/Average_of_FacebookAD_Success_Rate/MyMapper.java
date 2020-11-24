package org.hadoop.learning.SuccessRate.Facebook;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//FKLY490998LB,2010-01-29 06:12:17,Mumbai,Ecommerce,39,13,25-35   13/39 = 0.33
//NIXK800977XM,2010-01-03 00:01:20,Mumbai,Ecommerce,281,5,18-25   5/281 = 0.0177
//																  (0.33+0.0177)/2	

public class MyMapper extends Mapper<LongWritable,Text,Text,Text>{

	public void map(LongWritable Key,Text value,Context con) throws IOException,InterruptedException{
		
		String s=value.toString();
		String [] list_of_input=s.split(",");
		Text keyoutput=new Text(list_of_input[3]);
		Text outputvalue=new Text(list_of_input[2] + ","+list_of_input[4]+","+list_of_input[5]);
		con.write(keyoutput,outputvalue);		
	}
}
//Mapper will give sample output as Ecommerce : Mumbai,39,13 
