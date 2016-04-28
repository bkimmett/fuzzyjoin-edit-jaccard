package ca.uvic.csc.research;

import java.io.IOException;
import java.io.DataInput; // needed EXCLUSIVELY for IntArrayWritable
import java.io.DataOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/* This is a MapReduce class that describes a data structure for passing arrays of integers between MapReduce processes.
It behaves similarly to the regular MapReduce ArrayWritable class. */


public class IntArrayWritable implements WritableComparable<IntArrayWritable> { 
		private IntWritable[] values;
		
		public IntArrayWritable() { 
		}
		public IntArrayWritable(int length) {
			this.values = new IntWritable[length];
		} 
		public IntArrayWritable(IntWritable[] values) {
        	this.values = values;
   		}
   		/*public IntArrayWritable(Integer[] values) {
        	this.values = new IntWritable[values.length];
        	for(int i = 0; i < values.length; i++){
        		this.values[i] = new IntWritable(values[i]);
        	}
   		}*/
   		
   		// set() lets you pass in an entire array (of IntWritables-- not integers!), or just one index to change and the value to change it to.
   		
   		public void set(IntWritable[] values) { this.values = values; }
		public void set(int index, IntWritable value) { this.values[index] = value; }
		
		// get() lets you get the entire array, or just one index. getInt() lets you skip typecasting and get the integer value of one array element.
		
  		public IntWritable[] get() { return values; }
  		public IntWritable get(int index) { return values[index]; }
  		public int getInt(int index) { return (values[index]).get(); }
  		public int length() { return values.length; }
  		
  		// these are needed for mapReduce.
  		
  		public void readFields(DataInput in) throws IOException {
			values = new IntWritable[in.readInt()];          // construct values
			for (int i = 0; i < values.length; i++) {
			  values[i] = new IntWritable(in.readInt());                          // read a value + store it in values
			}
		  }

		 public void write(DataOutput out) throws IOException {
			out.writeInt(values.length);                 // write values
			for (int i = 0; i < values.length; i++) {
			  out.writeInt(values[i].get());
			}
		  }
		  
		  //compareTo: First value in the two arrays that's different wins by regular <, >.
		  
		  public int compareTo(IntArrayWritable o){
		  	int j;
		  	for (int i = 0; i < values.length; i++) {
		  		j = values[i].get() - o.values[i].get();
				if(j != 0){return j;}
			}
			return 0;
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
		  
		  //this returns an array of integers [of the IntArrayWritable's contents].
		  
		  public int[] toIntArray(){
		  	int[] result = new int[values.length];
		  	for (int i = 0; i < values.length; i++) {
		  		result[i] = values[i].get();
			}
		  	return result;
		  }
	}