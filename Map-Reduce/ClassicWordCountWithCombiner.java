package org.hadoop.learning;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ClassicWordCountWithCombiner {

	// Driver class configuration added in Main method
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		Configuration c = new Configuration();
		String [] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input= new Path(files[0]);
		Path output=new Path(files[1]);
		Job j = Job.getInstance(c,"ClassicWordCountWithCombiner");
		j.setJarByClass(ClassicWordCountWithCombiner.class);
		j.setMapperClass(WCMapper.class);
		j.setReducerClass(WCReducer.class);
		j.setCombinerClass(CombinerClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?1:0);
	}
	//Mapper Class
	public static class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key,Text value,Context con) throws IOException,InterruptedException{
		
			String s=value.toString();
			String [] words=s.split(",");
			
			for (String s1 : words) {
				Text outputkey=new Text(s1.toUpperCase().trim());
				IntWritable outputvalue=new IntWritable(1);
				con.write(outputkey, outputvalue);
			}
		}
	}
	//Reducer Class
	public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable>values,Context con) throws IOException,InterruptedException{
		
			int sum=0;
			for (IntWritable val: values) {
				sum+=val.get();
				
			}
			con.write(key, new IntWritable(sum));
		}
	}
	
	//Combiner Class will be same as Reducer class. We are extending reducer class and adding setCombinerClass in Driver code.
	public static class CombinerClass extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context con) throws IOException,InterruptedException{
			
			int sum=0;
			for(IntWritable val:values) {
				sum+=val.get();
			}
			con.write(key,new IntWritable(sum));
		}
	}

}

