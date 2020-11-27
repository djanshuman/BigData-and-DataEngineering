package org.hadoop.learning.OuterJoinReduceSide;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//1281,Shawn,Architect,7890,1481,10
// We are writing employee values as a single string with space as separator as values.
public class EmployeeMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key, Text value, Context con) throws IOException,InterruptedException{
		String s= value.toString().trim();
		String [] list_of_values=s.split(",");
		con.write(new Text(list_of_values[5]), new Text("EMPLOYEE" +","+list_of_values[0] +" "+list_of_values[1] + " "+ list_of_values[2] +" "+list_of_values[3]+" "+list_of_values[4]));
	}

}

// 10 : EMPLOYEE,1281 Shawn Architect 7890 1481
