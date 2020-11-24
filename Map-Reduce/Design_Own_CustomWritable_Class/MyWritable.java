package org.hadoop.learning.CustomWritableClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

// Custom writable class implementation. WritableComparable Interface is implemented as can be used both key and value whereas
//Writable Interface only supports value not key.

public class MyWritable implements WritableComparable<MyWritable>{

	private String word;
	
	public MyWritable() {
		set("");
	}
	public MyWritable(String word) {
		set(word);
	}
	public String getWord() {
		return this.word;
	}
	public void set(String word) {
		this.word = word;
	}
	
	//Mandatory functions for Hadoop to know how to write value for the class
	@Override
	public void write(DataOutput out) throws IOException{
		
		WritableUtils.writeString(out, this.word);
	}
	@Override
	public void readFields(DataInput in) throws IOException{
		this.word=WritableUtils.readString(in);
	}
	@Override
	public int compareTo(MyWritable o) {
		return this.word.compareTo(o.getWord());
	}
	@Override
	public String toString() {
		return this.word;
	}
}
