package org.hadoop.learning.OuterJoinReduceSide;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//10 : [{EMPLOYEE,1281 Shawn Architect 7890} {1481 DEPARTMENT, INVENTORY HYDERABAD}]
public class MyReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key , Iterable<Text> value, Context con) throws IOException, InterruptedException{
		
		List<String> employeeList= new ArrayList<String>();
		String dept="";
		
		Iterator<Text> itr=value.iterator();
		while(itr.hasNext()) 
		{
			Text data=itr.next(); //EMPLOYEE,1281 Shawn Architect 7890
			String [] list_of_values=data.toString().split(",");
			
			if(list_of_values[0].equalsIgnoreCase("EMPLOYEE")) {
				employeeList.add(list_of_values[1]);
			}
			else if (list_of_values[0].equalsIgnoreCase("DEPARTMENT")) {
				dept=list_of_values[1];
			}
		}
		// Crucial conditions, be careful in IF equal and not equal part and con.write part for left and right join.
		// For only Inner join requirement , comment out both left and right join block.
		// For Right Join requirement: Keep Inner Join+ Right join block, Comment out left join block.
		// For Left Join requirement : Keep Inner Join+ Left join block, Comment out Right join block.
		// For Full Outer Join: Keep all Inner + Right + Left join blocks.
		// We are keep inner join compulsory because it will give us the matched results in addition to outer joins.
		
			if(!employeeList.isEmpty() && !dept.isEmpty())  //Inner join
			{
				for(String emp : employeeList) {
					con.write(key, new Text(emp+ " "+dept));	
				}
			}
			
			if(!employeeList.isEmpty() && dept.isEmpty())  //Left Outer join
			{
				for(String emp : employeeList) {
					con.write(key, new Text(emp+ " " + "null_value null_value"));
					
				}
			}
			
			if(employeeList.isEmpty() && !dept.isEmpty())  //Right Outer join
			{
				con.write(key, new Text("null_value  null_value null_value null_value null_value"+ " "+dept));
			}
			
	}
}
