package org.hadoop.learning.BankLoyalCustomer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//OMOI808692OZ {[P,Allison,Abbott] , A,1245015582,3667,822,no}
public class MyReducer extends Reducer<Text,Text,Text,Text>{

	@Override
	public void reduce(Text key,Iterable<Text> values, Context con) throws IOException, InterruptedException {
		
		String f_name="";
		String l_name="";
		float totaldeposit=0;
		String account_number="";
		Iterator<Text> itr=values.iterator();
		
		while(itr.hasNext()){
			String []loop_string=itr.next().toString().split(","); // [P ,Allison,Abbott] // A,1245015582,3667,822,no
			
			//evaluates for person mapper output class value
			if (loop_string[0].equalsIgnoreCase("P")) {
				f_name=loop_string[1];
				l_name=loop_string[2];
			}
			//evaluates for account mapper output class value
			else if(loop_string[0].equalsIgnoreCase("A")) 
			{	account_number=loop_string[1];
			
				if(loop_string[4].equalsIgnoreCase("yes")) 
				{
					break;
				}
				else 
				{
					float withdraw_amount=Integer.parseInt(loop_string[3]);
					float deposit_amount=Integer.parseInt(loop_string[2]);
					//evaluates if withdraw amount is greater then total deposit of half quarter
					if(withdraw_amount >= (deposit_amount/2)) 
					{
						break;
					}
					else 
					{
						totaldeposit+=deposit_amount;
					}
				}
			}
		}
		//evaluates if total deposit is greater than 10k . Then it can be considered as loyal customer.
		if(totaldeposit >= 10000) {
			con.write(key, new Text(f_name +","+l_name+","+account_number+","+totaldeposit));
		}
		
	}
}
