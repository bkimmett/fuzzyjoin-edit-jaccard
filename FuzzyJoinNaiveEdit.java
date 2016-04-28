

package ca.uvic.csc.research;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.ID;

//import ca.uvic.csc.research.IntArrayWritable;
//import ca.uvic.csc.research.Text;
import ca.uvic.csc.research.EditDist; //edit distance library

import java.lang.Integer;
import java.lang.Long;
import java.lang.String;
//import java.util.HashMap;
import java.util.ArrayList;
//import java.io.File;
//import java.io.PrintWriter;


public class FuzzyJoinNaiveEdit {
	public static Path in_path;
	public static String in_name;
	public static int granularity;
	public static int threshold;
	
	//NOT YET FOR USE WITH BINARY STRINGS OF LENGTH > 32
	//with 132 reducers the highest granularity we can do is 15, using 120 reducers
	//
	
  	public static class FuzzyJoinNaiveEditMapper 
       extends Mapper<LongWritable, Text, ByteArrayWritable, Text>{
    
	private int granularity_mod; // determines how many buckets the strings are hashed to. G should ideally be a power of 2. The granularity G also specifies the number of reducers: It is always (G(G+1))/2 [so for G=4, you'd have 10 reducers].
    private ByteWritable key1;
    private ByteWritable[] keyOut;
    private Text valueOut;
    private String string_temp;

    
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
     Configuration conf = context.getConfiguration();
	 granularity_mod = conf.getInt("fuzzyJNaive.granularity", 1);
	 if(granularity_mod < 1){
	 granularity_mod = 1;
	 }
     
     String[] line = value.toString().trim().split("\\s",1);
     //expected: 1 value: the original string
	
	  /*if(line[0].length() > 32){
	  	throw new IOException("Error: a binary string is too long.");
	  }*/
      key1 = new ByteWritable((byte)Math.abs(line[0].hashCode() % granularity_mod)); // parse as binary, modulo appropriately
      Text valueOut;// = new Text(line[0]);
      keyOut = new ByteWritable[2];
      
      for(int j=0; j<granularity_mod; j++){ //output G copies to G reducers
      	if(key1.get() < j){
      		keyOut[0] = key1;
      		keyOut[1] = new ByteWritable((byte)j);
      		valueOut  = new Text("1"+line[0]);
      		//valueOut.setMeta((byte)1); //A side - you have to specify int.class or it'll get autoboxed to Integer :P
      	} else if (key1.get() > j) {
      		keyOut[0] = new ByteWritable((byte)j);
      		keyOut[1] = key1;
      		valueOut = new Text("2"+line[0]);
      		//valueOut.setMeta((byte)2); //B side
      	} else {
      		keyOut[0] = key1;
      		keyOut[1] = key1;
      		valueOut  = new Text("0"+line[0]);
      		//Integer dir = new Integer(0);
      		//valueOut.setMeta((byte)0); //both sides
      	}
       //System.err.println("Objects written: "+valueOut[0]+" and "+valueOut[1]);
      context.write(new ByteArrayWritable(keyOut), valueOut); //write string and hashed numeric directional value
      }
    }
  }
  
  public static class TriangleHashPartitioner extends Partitioner<ByteArrayWritable, Text>
	{
	   @Override
 	   public int getPartition(ByteArrayWritable key, Text value,int numReduceTasks)
 	   {	//the second value of key specifies the row. The first the column. The first is never greater than the second.
 	   		//to find the number of reducers in the previous N rows, you just need to find T_N, the triangular number for N rows.
 	   		//this is N(N+1)/2. Add the first value of the key for the reducer number to send to.
 	   	   int[] keyI = key.toIntArray();
 	   	   int prevRows = (keyI[1]*(keyI[1]+1))/2;
 	   	   int keyOut = prevRows+keyI[0];
 	   	   if(keyOut >= numReduceTasks){ 
 	   	   System.err.println("Partitioner says: reducing key "+keyOut+" to modulo "+numReduceTasks);
 	   	   keyOut = keyOut % numReduceTasks; // this is for systems where G(G+1)/2 reducers are not available [such as local]
 	   	   } 
 	       return keyOut;
 	   }  
	}
	
	 public static class FuzzyJoinNaiveNullReducer
       extends Reducer<ByteArrayWritable,Text,Text,Text> {
  	//dummy - reduces nothing. Used for measuring comm. cost
  		public void reduce(ByteArrayWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
                       return;
        }
  	}
  
  public static class FuzzyJoinNaiveEditReducer
       extends Reducer<ByteArrayWritable,Text,Text,Text> {

	private int distance_threshold; //how close two strings have to be to qualify. Higher = more permissive
	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;
	private static boolean null_absolute;
	private long records_written = 0;

	//final static int[] byte_ones = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4}; // lookup table for hamming distance between two bytes

	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;



    public void reduce(ByteArrayWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
		distance_threshold = conf.getInt("fuzzyJNaive.threshold", 1); 
		null_absolute = conf.getBoolean("fuzzyJNaive.null_absolute", false);              
        
        boolean hybrid_mode = true; //starts assuming it's getting in two sets of items and joining just those
                       
     	ArrayList<String> str_list_1 = new ArrayList<String>();
     	ArrayList<String> str_list_2 = new ArrayList<String>();
     	//System.err.println("reducer: ready to add!");
     	context.setStatus("reduce: reading data");
     	for (Text val : values) { // no need to keep a dict; the system tosses by key!!
     		//note: later duplicate records will overwrite earlier ones.
     		//int dir = val.toString().charAt(0);
     		String text = val.toString();
     		char dir = text.charAt(0);
     		text = text.substring(1);
     		
     		if(!hybrid_mode || dir == '0'){
     			str_list_1.add(text);
     			hybrid_mode = false;
     		} else if (dir == '1') {
     			str_list_1.add(text);
     		} else if (dir == '2') {
     			str_list_2.add(text);
     		} else {
     			System.err.println("Dir doesn't match!");
     		}
       	}
      
      //that's all data from the reducer read in. Next is processing it.
      	
      	
     	int list_1_size = str_list_1.size();
        int list_2_size = str_list_2.size();
     	String s1, s2;
     	 
		if(hybrid_mode){ // we're using two lists
		context.setStatus("reduce: joining data [two sets]");
			for(int i=0; i< list_1_size; i++){
				if(!null_absolute || i % 100 == 0){
					context.setStatus("reduce: joining data [two sets] ("+(i+1)+"/"+list_1_size+")");
				}
				for(int j=0; j< list_2_size; j++){
					s1 = str_list_1.get(i);
					s2 = str_list_2.get(j);
					if(s1.compareTo(s2) > 0){ //alphabetical order - THIS BLOCK IS OPTIONAL, it just makes for easily verifiable output.
						String swap = s1;
						s1 = s2; 
						s2 = swap;
					}	
					int t = EditDist.distanceTest(s1,s2,distance_threshold,1);
					if(t >= 0){
						if(!null_absolute){
							context.write(new Text(s1), new Text(s2+"  "+t)); // perform comparison. If a success, kick out the pair
						} else {
							records_written++;
						}
					}
				}
			}
		
		} else { // we're only using one
			context.setStatus("reduce: joining data [one set]");
			int t;
			for(int i=0; i< list_1_size; i++){
				if(!null_absolute || i % 1000 == 0){
					context.setStatus("reduce: joining data [one set] ("+(i+1)+"/"+list_1_size+")");
				}
				for(int j=i+1; j< list_1_size; j++){
					s1 = str_list_1.get(i);
					s2 = str_list_1.get(j);
					if(s1.compareTo(s2) > 0){ //alphabetical order - THIS BLOCK IS OPTIONAL, it just makes for easily verifiable output.
						String swap = s1;
						s1 = s2; 
						s2 = swap;
					}	
					t = EditDist.distanceTest(s1,s2,distance_threshold,1); 
					if(t >= 0){
						if(!null_absolute){
							context.write(new Text(s1), new Text(s2+"  "+t)); // perform comparison. If a success, kick out the pair
						} else {
							records_written++;
						}
					}
				/*current_chunk++;
				partial_chunks++;
				context.setStatus("reduce: processing chunk "+current_chunk+"/"+list_size+" ("+partial_chunks+"/"+total_chunks+" total)");*/
				}
			}
		}
      }
   }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length !=4) {
      System.err.println("Usage: FuzzyJoinNaiveEdit <in> <out> <comparison_threshold> <granularity>; granularity must be less than or equal to 2^(input string length). Setting <out> to 'null' discards output; 'null-cost' discards after copying to reducer.");
      System.exit(2);
    }
    //conf.setBoolean("mapred.compress.map.output", true);
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
    in_path = new Path(otherArgs[0]);

    
    granularity = Integer.parseInt(otherArgs[3]);
    conf.setInt("fuzzyJNaive.granularity", granularity);
    threshold = Integer.parseInt(otherArgs[2]);
    conf.setInt("fuzzyJNaive.threshold", threshold);
  
  	if(otherArgs[1].equals("null-absolute")){
  		conf.setBoolean("fuzzyJNaive.null_absolute", true);
    }
  
  
    Job job = new Job(conf, "Fuzzy Join [Binary Strings, Naive: threshold "+threshold+"]");
	job.setNumReduceTasks((granularity*(granularity+1))/2); 
	//job.setNumReduceTasks(0); 
    job.setJarByClass(FuzzyJoinNaiveEdit.class);
    job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
    job.setMapperClass(FuzzyJoinNaiveEditMapper.class);
    job.setPartitionerClass(TriangleHashPartitioner.class);
    //reducer set below
    job.setMapOutputKeyClass(ByteArrayWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   	FileInputFormat.addInputPath(job, in_path);
   	if(otherArgs[1].equals("null")){
		System.err.println("Note: ALL OUTPUT DISCARDED");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinNaiveEditReducer.class);
	} else if (otherArgs[1].equals("null-cost")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (measuring communication cost)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinNaiveNullReducer.class);
	} else if (otherArgs[1].equals("null-absolute")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (completely)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinNaiveEditReducer.class);	
	} else {
		job.setReducerClass(FuzzyJoinNaiveEditReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	}
    
  
    boolean success = job.waitForCompletion(true);
    if(!success){ System.exit(1); }
    /*TaskCompletionEvent[] outputs = job.getTaskCompletionEvents(0);
    File outlog = new File("logs/"+job.getJobID()+"_times");
    outlog.createNewFile();
    PrintWriter outline = new PrintWriter(outlog);
    
    int completionOffset = 0;
    do{
    for( TaskCompletionEvent task: outputs){
    	outline.println(task.getTaskAttemptId()+"\t"+task.getTaskStatus()+"\t"+((float)task.getTaskRunTime()/1000.0));
    }
    	completionOffset += outputs.length;
    	outputs = job.getTaskCompletionEvents(completionOffset);
    } while(outputs.length > 0);
    outline.println(completionOffset);
    outline.close();*/
    
    System.exit(0);
  }
}
