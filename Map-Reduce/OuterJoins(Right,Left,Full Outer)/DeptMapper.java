package org.hadoop.learning.OuterJoinReduceSide;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//10,INVENTORY,HYDERABAD
// We are writing dept values as a string with space as separator as a single string.
public class DeptMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key, Text value, Context con) throws IOException,InterruptedException{
		String s= value.toString().trim();
		String [] list_of_values=s.split(",");
		con.write(new Text(list_of_values[0]), new Text("DEPARTMENT" +","+list_of_values[1] + " "+ list_of_values[2]));
	}
}

//10 : DEPARTMENT, INVENTORY HYDERABAD
