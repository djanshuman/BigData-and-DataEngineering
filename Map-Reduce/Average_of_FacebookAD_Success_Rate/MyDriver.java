package org.hadoop.learning.SuccessRate.Facebook;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.hadoop.learning.SuccessRate.Facebook.MyMapper;
import org.hadoop.learning.SuccessRate.Facebook.MyReducer;
public class MyDriver {
	
	public static void main (String [] args) throws Exception{
		
		Configuration c = new Configuration();
		String [] files = new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = Job.getInstance(c,"MyDriver");
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?1:0);
	}
}
