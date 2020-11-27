package org.hadoop.learning.InnerJoinReduceSide;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//1 , [{EMP,JACK} {ADDRESS,PARIS}]

public class MyReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key , Iterable<Text> value, Context con) throws IOException, InterruptedException{
		
		List<String> employeeList= new ArrayList<String>();
		List<String> addressList= new ArrayList<String>();
		
		Iterator<Text> itr= value.iterator();
		while(itr.hasNext()) {
			Text data=itr.next(); //EMP,JACK
			String [] list_of_values= data.toString().split(","); //[{EMP} {JACK}]
			
			//based on type of value we are adding to separate list.
			if (list_of_values[0].equalsIgnoreCase("EMPLOYEE")) {
				employeeList.add(list_of_values[1]);
			}
			else if (list_of_values[0].equalsIgnoreCase("ADDRESS")) {
				addressList.add(list_of_values[1]);
			}
			else {
				System.out.println("Invalid Employee and Address details");
			}
		}
		
		//Traverse and for each row add based on the emp element add the corresponding address from address list. 
		//In Inner join both list should not be empty as we are joining on a common key
		
		if(!employeeList.isEmpty() && !addressList.isEmpty()) {
			
			for (String emp : employeeList) {
				
				for (String add : addressList) {
					
					con.write(new Text(key), new Text(emp +","+add));
				}
			}
		}
		
	}
}
