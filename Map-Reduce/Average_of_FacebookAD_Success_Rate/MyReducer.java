package org.hadoop.learning.SuccessRate.Facebook;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Mapper will give sample output as Ecommerce : Mumbai,39,13 
//{Ecommerce : Mumbai,39,13 , Ecommerce : Mumbai,281,5}

public class MyReducer extends Reducer<Text,Text,Text,Text>{
	@Override
	protected void reduce(Text key,Iterable<Text> values,Context con) throws IOException,java.lang.InterruptedException{
		
		HashMap <String,String> citydata=new HashMap<String, String>(); // {Mumbai: 33.3,1} , After 2nd run{Mumbai: 35.07,2}
		Iterator<Text> itr=values.iterator();
		
		while(itr.hasNext()) {
			
			String f=itr.next().toString(); // Mumbai,281,5
			String [] list_of_values=f.split(",");
			String location=list_of_values[0].trim();
			int clickCount=Integer.parseInt(list_of_values[1]);  //281
			int conversionCount	=Integer.parseInt(list_of_values[2]); //5
			Double succRate= new Double(conversionCount/(clickCount*1.0)*100); // 1.77
			
			if(citydata.containsKey(location)) {
				String s1=citydata.get(location);
				String []s2= s1.split(",");
				Double totalSuccRate=Double.parseDouble(s2[0])+succRate;
				int totalcount=Integer.parseInt(s2[1])+1;
				citydata.put(location, totalSuccRate+ ","+totalcount);
			}
			else {
				citydata.put(location, succRate+",1");   //{Mumbai: 33.3,1}
			}
		}
		System.out.println(citydata.toString());
		for (Map.Entry<String, String> e:citydata.entrySet()) { // e=Mumbai value=35.07,2
			
			String [] s3=e.getValue().split(",");
			Double avgsuccRate=Double.parseDouble(s3[0])/Integer.parseInt(s3[1]);
			con.write(key, new Text(e.getKey() +","+ avgsuccRate));
		}	
	}
}
