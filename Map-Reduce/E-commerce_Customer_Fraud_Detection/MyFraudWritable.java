package org.hadoop.learning.FraudCustomersDetection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MyFraudWritable implements Writable{

	private String customername;
	private String receivedDate;
	private boolean returned;
	private String returnedDate;
	
	public MyFraudWritable() {
		set("","","no","");
	}
	public void set(String customername,String receivedDate,String returned,String returnedDate) {
		this.customername=customername;
		this.receivedDate=receivedDate;
		if(returned.equalsIgnoreCase("yes")) {
			this.returned=true;
		}
		else {
			this.returned=false;
		}
		this.returnedDate=returnedDate;
	}

	public String getCustomername() {
		return this.customername;
	}
	
	public String getReceivedDate() {
		return this.receivedDate;
	}

	public boolean getReturned() {
		return this.returned;
	}

	public String getReturnedDate() {
		return this.returnedDate;
	}
	
	//Compulsory functions to be override for Hadoop to understand the Writable class
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, this.customername);
		WritableUtils.writeString(out, this.receivedDate);
		out.writeBoolean(this.returned);
		WritableUtils.writeString(out, this.returnedDate);
	}
	@Override
	public void readFields(DataInput in) throws IOException{
		this.customername=WritableUtils.readString(in);
		this.receivedDate=WritableUtils.readString(in);
		this.returned=in.readBoolean();
		this.returnedDate=WritableUtils.readString(in);		
	}
}
