package ca.uvic.csc.research;

import java.io.IOException;
import java.io.DataInput; // needed EXCLUSIVELY for RangeIntArrayWritable
import java.io.DataOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/* This is a MapReduce class that describes a data structure for passing arrays of integers between MapReduce processes.
It behaves similarly to the regular MapReduce ArrayWritable class. */


public class RangeIntArrayWritable implements WritableComparable<RangeIntArrayWritable> { 
		private IntWritable[] values;
		private int range_lower, range_upper;
		
		public RangeIntArrayWritable() { 
			this.range_lower = 0;
			this.range_upper = 0;
		}
		public RangeIntArrayWritable(int length) {
			this.values = new IntWritable[length];
			this.range_lower = 0;
			this.range_upper = 0;
		} 
		public RangeIntArrayWritable(IntWritable[] values) {
        	this.values = values;
        	this.range_lower = 0;
			this.range_upper = 0;
   		}
   		public RangeIntArrayWritable(IntWritable[] values, int lower, int upper) {
        	this.values = values;
        	this.range_lower = lower;
			this.range_upper = upper;
   		}
   		
   		// set() lets you pass in an entire array (of IntWritables-- not integers!), or just one index to change and the value to change it to.
   		
   		public void set(IntWritable[] values) { this.values = values; }
		public void set(int index, IntWritable value) { this.values[index] = value; }
		public void setLower(int value){ this.range_lower = value; }
		public void setUpper(int value){ this.range_upper = value; }
		public void setRange(int lower, int upper){ 
			this.range_lower = lower; 
			this.range_upper = upper;
		}
		
		// get() lets you get the entire array, or just one index. getInt() lets you skip typecasting and get the integer value of one array element.
		
  		public IntWritable[] get() { return values; }
  		public IntWritable get(int index) { return values[index]; }
  		public int getInt(int index) { return (values[index]).get(); }
  		public int[] getRange() {return new int[]{range_lower, range_upper};}
  		public int getLower() {return range_lower;}
  		public int getUpper() {return range_upper;}
  		public int length() { return values.length; }
  		
  		// these are needed for mapReduce.
  		
  		public void readFields(DataInput in) throws IOException {
			values = new IntWritable[in.readInt()];          // construct values
			for (int i = 0; i < values.length; i++) {
			  values[i] = new IntWritable(in.readInt());                          // read a value + store it in values
			}
			range_lower = in.readInt();
			range_upper = in.readInt();
		  }

		 public void write(DataOutput out) throws IOException {
			out.writeInt(values.length);                 // write values - writeInt(int) writes an int
			for (int i = 0; i < values.length; i++) {
			  out.writeInt(values[i].get());
			}
			out.writeInt(range_lower);
			out.writeInt(range_upper);
		  }
		  
		  //compareTo: First value in the two arrays that's different wins by regular <, >.
		  //empty is < all as an elemnt.
		  public int compareTo(RangeIntArrayWritable o){
		  	int j;
		  	int len = (values.length <= o.length())?values.length:o.length();
		  	for (int i = 0; i < len; i++) {
		  		j = values[i].get() - o.getInt(i).get();
				if(j != 0){return j;}
			}
			if(values.length != o.length()){
				return values.length - o.length();
			}
			if(this.range_lower != o.getLower()){
				return this.range_lower - o.getLower(); //range tiebreaks, first lower bound...
			} else {
				return this.range_upper - o.getUpper();//then upper.
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
		  
		   public static class Comparator extends WritableComparator {
			public Comparator() {
			  super(RangeIntArrayWritable.class);
			}

			public int compare(RangeIntArrayWritable a, RangeIntArrayWritable b) {
			  int thisValue = readInt(b1, s1);
			  int thatValue = readInt(b2, s2);
			  return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
			}
		  }
		  
		  
		  //this returns an array of integers [of the RangeIntArrayWritable's contents].
		  
		  public int[] toIntArray(){
		  	int[] result = new int[values.length];
		  	for (int i = 0; i < values.length; i++) {
		  		result[i] = values[i].get();
			}
		  	return result;
		  }
	}