package org.hadoop.learning.MultipleOutputClass;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//{DJPX255251, [Arthur,HR,6597] ,[Arthur,HR,6397], [Arthur,HR,6797]}

public class MyReducer extends Reducer<Text,Text,Text,Text>{
	
	// This is the format for having multipleoutput . Create MultipleOutputs object and pass context object in setup method.
	private MultipleOutputs<Text,Text> obj;
	
	@Override
	protected void setup(Context con) throws IOException, InterruptedException {
		
		obj= new MultipleOutputs(con);
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context con) throws IOException, InterruptedException{
		String name="";
		String dept="";
		double salary=0;
		
		Iterator<Text> itr=values.iterator();
		while(itr.hasNext()) {
			String s= itr.next().toString();
			String[] list_of_values= s.split(",");
			name=list_of_values[0];
			dept=list_of_values[1];
			salary+=Double.parseDouble(list_of_values[2].trim());
		}
			// Separating files based on department
			if(dept.equalsIgnoreCase("HR")) {
				obj.write("HR", key, new Text(name +","+salary));
			}
			else if (dept.equalsIgnoreCase("Accounts")) {
				obj.write("Accounts", key, new Text(name +","+salary));
			}
			else {
				System.out.println("Invalid Department name reported for Employee :" +name);
			}
		
	}
	//cleanup method compulsory to close the MultipleOutputs object created in setup method.
	protected void cleanup(Context con) throws IOException, InterruptedException {
		
		obj.close();
	}
}
