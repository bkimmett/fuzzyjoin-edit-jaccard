package ca.uvic.csc.research;

import java.io.IOException;
import java.io.DataInput; // needed EXCLUSIVELY for MetadataIntArrayWritable
import java.io.DataOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/* This is a MapReduce class that describes a data structure for passing arrays of integers between MapReduce processes.
It behaves similarly to the regular MapReduce ArrayWritable class. */


public class MetadataIntArrayWritable implements WritableComparable<MetadataIntArrayWritable> { 
		private IntWritable[] values;
		private byte meta;
		private int tag;
		
		public MetadataIntArrayWritable() { 
			this.meta = 0x00;
			this.tag = 0;
		}
		public MetadataIntArrayWritable(int length) {
			this.values = new IntWritable[length];
			this.meta = 0x00;
			this.tag = 0;
		} 
		public MetadataIntArrayWritable(IntWritable[] values) {
        	this.values = values;
        	this.meta = 0x00;
        	this.tag = 0;
   		}
   		public MetadataIntArrayWritable(IntWritable[] values, byte meta) {
        	this.values = values;
        	this.meta = meta;
        	this.tag = 0;
   		}
   		public MetadataIntArrayWritable(IntWritable[] values, int tag) {
        	this.values = values;
        	this.meta = 0x00;
        	this.tag = tag;
   		}
   		public MetadataIntArrayWritable(IntWritable[] values, int tag, byte meta) {
        	this.values = values;
        	this.meta = meta;
        	this.tag = tag;
   		}
   		
   		// set() lets you pass in an entire array (of IntWritables-- not integers!), or just one index to change and the value to change it to.
   		
   		public void set(IntWritable[] values) { this.values = values; }
		public void set(int index, IntWritable value) { this.values[index] = value; }
		public void setMeta(byte value){ this.meta = value; }
		public void setTag(int value){ this.tag = value; }
		
		// get() lets you get the entire array, or just one index. getInt() lets you skip typecasting and get the integer value of one array element.
		
  		public IntWritable[] get() { return values; }
  		public IntWritable get(int index) { return values[index]; }
  		public int getInt(int index) { return (values[index]).get(); }
  		public byte getMeta() {return meta;}
  		public int getTag() {return tag;}
  		public int length() { return values.length; }
  		
  		// these are needed for mapReduce.
  		
  		public void readFields(DataInput in) throws IOException {
			values = new IntWritable[in.readInt()];          // construct values
			for (int i = 0; i < values.length; i++) {
			  values[i] = new IntWritable(in.readInt());                          // read a value + store it in values
			}
			tag = in.readInt();
			meta = in.readByte();
		  }

		 public void write(DataOutput out) throws IOException {
			out.writeInt(values.length);                 // write values - write(int) writes an int
			for (int i = 0; i < values.length; i++) {
			  out.writeInt(values[i].get());
			}
			out.writeInt(tag);
			out.writeByte(meta);
		  }
		  
		  //compareTo: First value in the two arrays that's different wins by regular <, >.
		  
		  public int compareTo(MetadataIntArrayWritable o){
		  	int j;
		  	for (int i = 0; i < values.length; i++) {
		  		j = values[i].get() - o.values[i].get();
				if(j != 0){return j;}
			}
			if(this.meta != o.getMeta()){
				return Byte.valueOf(this.meta).compareTo(o.getMeta()); //metadata tiebreaks
			} else {
				return Integer.valueOf(this.tag).compareTo(o.getTag()); //if meta is equal, tag tiebreaks
			}
		  }
		  public String toString(){
		  	String result = "";
		  	int len = values.length;
		  	for (int i = 0; i < len; i++) {
		  		result += values[i].get();
		  		if(i < len-1){
		  			result += " ";
		  		}
			}
			return result;
		  }
		  
		  //this returns an array of integers [of the MetadataIntArrayWritable's contents].
		  
		  public int[] toIntArray(){
		  	int[] result = new int[values.length];
		  	for (int i = 0; i < values.length; i++) {
		  		result[i] = values[i].get();
			}
		  	return result;
		  }
	}