package org.hadoop.learning.BankLoyalCustomer;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyDriver {
	
	public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration c= new Configuration();
		Path accountPath= new Path("hdfs://localhost:8020/user/resources/BankLoyalCustomer_AccountInput/account.txt");
		Path personPath= new Path("hdfs://localhost:8020/user/resources/BankLoyalCustomer_PersonInput/person.txt");
		Path outputpath= new Path("hdfs://localhost:8020/user/resources/BankLoyalCustomer_Output");
		
		Job j = Job.getInstance(c,"MyDriver");
		j.setJarByClass(MyDriver.class);
		j.setMapperClass(AccountMapper.class);
		j.setMapperClass(PersonMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		
		//MultipleInput class required for identifying and merging during job/task execution
		MultipleInputs.addInputPath(j, accountPath, TextInputFormat.class, AccountMapper.class);
		MultipleInputs.addInputPath(j, personPath, TextInputFormat.class,PersonMapper.class);
		
		
		FileOutputFormat.setOutputPath(j,outputpath);
		System.exit(j.waitForCompletion(true)?1:0);
		
		
	}

}
