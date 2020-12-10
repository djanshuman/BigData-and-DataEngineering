package org.hadoop.learning.ChurnCustomerDetection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// JXJY167254JK [{18-06-2017,2,Late delivery},{07-06-2017,3,Late delivery} ,{20-06-2017,3,Stale food}]
public class MyReducer extends Reducer<Text,Text,Text,Text> {
	@Override
	protected void reduce(Text key,Iterable<Text> values, Context con) throws IOException, InterruptedException {
		
		//HashMap to store orderCount month wise and specific feedback count by order
		HashMap<Integer,Integer> ordersCountByMonth = new HashMap<Integer,Integer>(); //06 10 08 6 09 3 
		HashMap<String,Integer> feedbackCountByOrder = new HashMap<String,Integer>(); //Late delivery 1
		
		Iterator<Text> itr=values.iterator();
		while(itr.hasNext()) {
			
			String [] order=itr.next().toString().split(","); //[18-06-2017,2,Late delivery]
			int orderMonth=Integer.parseInt(order[0].split("-")[1].trim());
			int ratings=Integer.parseInt(order[1].trim());
			String feedback=order[2].trim();
			
			if (ordersCountByMonth.containsKey(orderMonth)) 
			{
				ordersCountByMonth.put(orderMonth, ordersCountByMonth.get(orderMonth)+1);
			}
			else 
			{
				ordersCountByMonth.put(orderMonth, 1);
			}
			if(ratings <=3) 
			{
				if(feedbackCountByOrder.containsKey(feedback))
				{
					feedbackCountByOrder.put(feedback, feedbackCountByOrder.get(feedback)+1);
				}
				else
				{
					feedbackCountByOrder.put(feedback, 1);
				}
				
			}
		}
		System.out.println("****************************");
		System.out.println(key.toString());
			
		int previousMonthOrder=0;
		double orderRate=0;
		int declineCount=0;
			
		for (int i=1;i<=12;i++) {
				
			Integer currMonthOrders=ordersCountByMonth.get(i);
			
			if (currMonthOrders == null) {
				currMonthOrders=0;
					
			}
			if(previousMonthOrder > 0) {
				orderRate=((1.0*currMonthOrders)/previousMonthOrder)*100; 
			}
			else {
				orderRate=0;
			}
			if((previousMonthOrder > currMonthOrders) && (orderRate <50.0)) {
				declineCount++;
			}
			else {
				declineCount=0;
			}
			System.out.println(previousMonthOrder + " , " + currMonthOrders +" , "+ orderRate +" , "+declineCount);
				
			if (declineCount ==3 ) {
				String feedbackReason="";
				int feedbackCount=0;
				for (Map.Entry<String, Integer> e : feedbackCountByOrder.entrySet()) {
						
					if(e.getValue() > feedbackCount) {
						feedbackReason=e.getKey();
						feedbackCount=e.getValue();
					}
						
				}
				con.write(key, new Text(feedbackReason));
					
			}
			previousMonthOrder=currMonthOrders;
		}
	}
	
}
