package org.hadoop.learning.OuterJoinReduceSide;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {

	public static void main (String [] args) throws IOException , InterruptedException, ClassNotFoundException{
		
		Configuration c = new Configuration();
		Path input1 = new Path("hdfs://localhost:8020/user/resources/OuterJoinReduceSideInput1");
		Path input2 = new Path("hdfs://localhost:8020/user/resources/OuterJoinReduceSideInput2");
		Path outputpath= new Path("hdfs://localhost:8020/user/resources/OuterJoinReduceSideOutput");
		Job j = Job.getInstance(c,"MyDriver");
		j.setJarByClass(MyDriver.class);
		j.setMapperClass(EmployeeMapper.class);
		j.setMapperClass(DeptMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		
		//Multipleinput 
		MultipleInputs.addInputPath(j, input1, TextInputFormat.class,EmployeeMapper.class);
		MultipleInputs.addInputPath(j, input2, TextInputFormat.class,DeptMapper.class);
		FileOutputFormat.setOutputPath(j, outputpath);
		System.exit(j.waitForCompletion(true)?1:0);
	}
}

