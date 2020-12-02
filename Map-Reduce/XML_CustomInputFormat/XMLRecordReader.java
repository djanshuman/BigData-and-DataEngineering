package org.hadoop.learning.CustomInputFormatter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class XMLRecordReader extends RecordReader<LongWritable,Text>{

	public final String startTag= "<MOVIES>";
	public final String endTag= "</MOVIES>";
	
	private LineReader linereader;
	
	private long curr_position=0;
	private long startofFile;
	private long endofFile;
	
	private LongWritable key=new LongWritable();
	private Text value= new Text();
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext c) throws IOException, InterruptedException
	{
		//1. Here we are assigning the current inputsplit to the FileSplit object.
		FileSplit fs1= (FileSplit) inputSplit;
		Configuration con =c.getConfiguration();
		
		//2. Start of inputsplit
		startofFile=fs1.getStart();
		//3. End of inputplit total length of characters
		endofFile=startofFile+fs1.getLength();
		
		//4. Fetching the path of split in HDFS
		Path file=(Path) fs1.getPath();
		//5. Getting filesystem permission to access the inputsplit present in path
		FileSystem fs=file.getFileSystem(con);
		
		//6.Opening the stream to read lines
		FSDataInputStream fsin= fs.open(fs1.getPath());
		fsin.seek(startofFile);
		
		//7.Linereader now points to the input split first line or current_position
		linereader=new LineReader(fsin,con);
		
		//8. Assigning current position marker to read from start of file or 0th character.
		this.curr_position=startofFile;
	
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		
		key.set(curr_position);
		//Ensuring to clear all values if set previously
		value.clear();
		
		Text line=new Text();
		boolean startFound=false;
		
		while(curr_position < endofFile) 
		{
			// Here linereader is pointing and reading first line and entire length of characters are stored.
			long linelength=linereader.readLine(line);
			curr_position=curr_position+linelength;
			
			if(!startFound && line.toString().equalsIgnoreCase(this.startTag)) {
				startFound=true;
			}
			else if (startFound && line.toString().equalsIgnoreCase(this.endTag)) 
			{
				// as we have reached endtag so we are removing comma from the end of string and setting
				String withoutComma=value.toString().substring(0, value.toString().length()-1);
				value.set(withoutComma);
				return true;
			}
			else if(startFound) 
			{
				
				String s1=line.toString();
				//reading the entire word and removing xml tags
				String content=s1.replaceAll("<[^>]+>", "");
				//adding comma to each word read
				value.append(content.getBytes("utf-8"), 0, content.length());
				value.append(",".getBytes("utf-8"), 0, ",".length());
			}
			
		}
		return false;
	
	}
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException{
		
		return key;
	}
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException{
		return value;
	}
	
	//to keep track of mapper progress 10%,20% completed.
	@Override
	public float getProgress() throws IOException, InterruptedException{
		return (curr_position-startofFile)/(float)(endofFile-startofFile);
	}
	//below function close all open connections or objects
	@Override
	public void close() throws IOException {
		if (linereader!=null) {
			linereader.close();
		}
	}
	
}
