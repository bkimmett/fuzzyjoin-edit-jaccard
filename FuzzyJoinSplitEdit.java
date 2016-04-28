

package ca.uvic.csc.research;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.ID;

import org.apache.commons.lang.ArrayUtils; //packed with Hadoop

//import ca.uvic.csc.research.IntArrayWritable;
//import ca.uvic.csc.research.MetadataShortWritable;
import ca.uvic.csc.research.ACKeywordTree; //keyword tree - used for lexical analysis
import ca.uvic.csc.research.EditDist; //edit distance library

import java.lang.Integer;
import java.lang.Long;
import java.lang.String;
import java.util.HashSet;
import java.util.ArrayList;
//import java.io.File;
//import java.io.PrintWriter;


public class FuzzyJoinSplitEdit {
	public static Path in_path;
	public static boolean first_string = true;
	private static int threshold;
	
	private static int string_length; 
	

	
  	public static class FuzzyJoinSplitEditMapper 
       extends Mapper<LongWritable, Text, Text, Text>{
    
    private Text keyOut;
    private Text valueOut;
	private static boolean chunks_set = false;
	private static int threshold = 0;
    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
     Configuration conf = context.getConfiguration();
	 
     int lower_chunk_window_size, upper_chunk_window_size;
     
      String[] line = value.toString().trim().split("\\s",1);
      //expected: 1 value: the original string
	
	 String value_to_substring = line[0];
	 valueOut = new Text(value_to_substring);
	 
	 if(threshold == 0){
	 	threshold = conf.getInt("fuzzyJSplit.threshold", 1);
	 }
	 //figure out 'sliding chunk window' size for this string
	 
	 if(value_to_substring.length() <= threshold){
	 	lower_chunk_window_size = 0; //too short
	 } else {
	 	lower_chunk_window_size = (value_to_substring.length()-threshold)/(threshold+1);
	 	//for stringlen = 32, threshold = 10 gives chunk size of 2. Bigger gives chunk size of 1.
	 }
	 upper_chunk_window_size = value_to_substring.length()/(threshold+1);
	 //threshold = 15 givs chunk size of 2. Bigger gives chunk size of 1.
	 
	 if(lower_chunk_window_size == 0 && upper_chunk_window_size == 0){
	 	 upper_chunk_window_size = 1; //we want to include a '1' case for strings bigger than threshold size
	 } else if(lower_chunk_window_size == 0) {
	 	lower_chunk_window_size = 1; //but strings just bigger don't get a 0 case!
	 }
	 //lower_chunk_window_size and upper_chunk_window_size CAN be equal. Typically this happens at string lengths where length = n*(threshold+1)-1 for some value of n.
	 
	 String this_chunk = "";
	 
	 for(int len = lower_chunk_window_size; len <= upper_chunk_window_size; len++){
	 	if(len == 0){
	 		//write this key: "" - YA RLY.
	 		//write this value: the full string. chunk location is null.
	 		context.write(new Text(""),valueOut);
	 	} else {
	 		HashSet<String> chunks = new HashSet<String>(((value_to_substring.length()-len)*4)/3);
	 		//System.err.println("String is "+value_to_substring.length()+" chars long. Chunk size: "+len);
	 		for(int start = 0; start <= value_to_substring.length()-len; start++){
	 			//write this key: the string chunk, obviously.
	 			//write this value: the full string! also the location of the chunk
	 			//System.err.println("Substringing from "+start+" to "+(start+len)+".");
	 			this_chunk = value_to_substring.substring(start,start+len);
	 			if(chunks.add(this_chunk)){
	 				context.write(new Text(this_chunk),valueOut);
	 			} //otherwise it's found already, skip
	 		}
	 	}
	 }
      }
    }
  
   public static class ChunkHashingPartitioner extends Partitioner<Text, Text>{
	
	   @Override
 	   public int getPartition(Text key, Text value,int numReduceTasks)
 	   {	
 	   	   return Math.abs(key.toString().hashCode()) % numReduceTasks; // simple modulo hashing for relatively equal distribution. 
 	   }  
	}

	public static class FuzzyJoinSplitNullReducer
       extends Reducer<Text,Text,Text,Text> {
       //returns without reducing anything. Used for measuring communication cost.
		public void reduce(Text key, Iterable<Text> values, 
		   Context context
		   ) throws IOException, InterruptedException {
	   
		  	 return;
		   }
  
  }
  
  
  public static class FuzzyJoinSplitEditReducer
       extends Reducer<Text,Text,Text,Text> {
       
       
    private static int[] string_chunks;
	private static int[] shift_positions;
	private static boolean chunks_set = false;
	private static int threshold = 0;
	private static boolean null_absolute;
	private long records_written = 0;

	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;

	private boolean substring_verify(String one, String two, int chunk_len){ //check that all preceding chunks of the strings are not equal
		if(chunk_len == 0){
			//this is an interesting and kind of awkward situation. It's entirely possible that we might get duplicates... have to see.
			//return true;
			chunk_len = 1;
		}
		if(one.length() == 0 || two.length() == 0){
			//trivial case; we're go, as there is no match
			return true;
		}	
		if(one.length() < two.length()){ //longest string first please
			String swap = one;
			one = two;
			two = swap;
		}
		ACKeywordTree<Character> searchTree = new ACKeywordTree<Character>();
		
		searchTree.addChunkedKeyword(ArrayUtils.toObject(one.toCharArray()),chunk_len);
		
		return !(searchTree.testAllKeywordsShortCircuit(ArrayUtils.toObject(two.toCharArray())));
		/*for(int i = family-1; i >= 0; i--){ //check i'th substring
			if((one & string_chunks[i]) == (two & string_chunks[i])){ return false;}
		}
		return true;*/
	}


    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        //System.err.println("Reducer's ready.");
        null_absolute = conf.getBoolean("fuzzyJSplit.null_absolute", false);
        if (threshold == 0){
        	threshold = conf.getInt("fuzzyJSplit.threshold", 1);
        }	
        String key_t = key.toString();
        int keyLen = key_t.length();
        String current;
     	ArrayList<String> str_list = new ArrayList<String>();
     	ArrayList<String> str_list_chopped = new ArrayList<String>();
     	//System.err.println("reducer: ready to add!");
     	context.setStatus("reduce: reading data");
     	// System.err.println("Key: "+key);
     	for (Text val : values) { // no need to keep a dict; the system tosses by key!!
     		//note: later duplicate records will overwrite earlier ones.
     		current = val.toString();
     		if(keyLen == 0 && current.length() > threshold){ //special case: only strings of length <= threshold may pass
     			continue;
     		}
     		str_list.add(current);
     		if(keyLen == 0){
     			//str_list_chopped.add(current); //don't bother, we won't use it   //we find the key immediately, so...
     		} else {
     			str_list_chopped.add(current.substring(0,keyLen+current.indexOf(key_t)-1)); //add the chunk that's before the key for lexical analysis later
     		  //System.err.println("String: "+current+" / Key: "+key_t+" / Left: "+current.substring(0,keyLen+current.indexOf(key_t)-1));
     		}
     		//System.err.println("Value: "+val.get());
       	}
      
      //that's all data from the reducer read in. Next is processing it.
      	
      	
     	 int list_size = str_list.size();
     	 int t;
     	 String s1, s2, s1_c;
     	 
     	context.setStatus("reduce: joining data");
     	if(keyLen > 0){
     		//normal behavior
			for(int i=0; i< list_size; i++){
				if(!null_absolute || i % 100 == 0){
					context.setStatus("reduce: joining data ("+(i+1)+"/"+list_size+")");
				}
				
				for(int j=i+1; j< list_size; j++){
					s1 = str_list.get(i);
					s2 = str_list.get(j);
					s1_c = str_list_chopped.get(i);
					if(s1.compareTo(s2) > 0){ //alphabetical order - swap strings
						String swap = s1;
						s1 = s2; 
						s2 = swap;
						s1_c = str_list_chopped.get(j);
					}
			
					t = EditDist.distanceTest(s1,s2,threshold,1);
					if(t >= 0 && substring_verify(s1_c,s2,keyLen)){ // if the distance checks out and the strings are lexicographically first...
						if(!null_absolute){
							context.write(new Text(s1), new Text(s2+"  "+t/*+"  "+keyLen+"  "+key_t+" "+s1_c+" "+s2*/));
							// perform comparison. If a success, kick out the pair
						} else {
							records_written++;
						}
					}
				}
			}
		} else {
			//0-length chunks - JOIN ALL PAIRS INDISCRIMINATELY... sort of.
			//for a pair to be output here, the two words must have NOTHING in common - otherwise, the 1-length segment'll take care of 'em. As such, the distance must equal the longest of the two words: every char was changed.
			
			for(int i=0; i< list_size; i++){
				if(!null_absolute || i % 100 == 0){
					context.setStatus("reduce: joining data ("+(i+1)+"/"+list_size+")");
				}
				for(int j=i+1; j< list_size; j++){
					s1 = str_list.get(i);
					s2 = str_list.get(j);
					if(s1.compareTo(s2) > 0){ //alphabetical order
						String swap = s1;
						s1 = s2; 
						s2 = swap;
					}	
					t = EditDist.distanceTest(s1,s2,threshold,1); 
					if(t >= Math.max(s1.length(),s2.length())&& substring_verify(s1,s2,1)){ //'nothing in common' check
						if(!null_absolute){
							context.write(new Text(s1), new Text(s2+"  "+t/*+"  "+keyLen*/));
							// perform comparison. If a success, kick out the pair
						} else {
							records_written++;
						}
					}
				}
			}
			
		}
      }
   }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length <4) {
      System.err.println("Usage: FuzzyJoinSplitEdit <in> <out> <comparison_threshold> <number_of_reducers>;  Setting <out> to 'null' discards output; 'null-cost' discards after copying to reducer.");
      System.exit(2);
    }
    //conf.setBoolean("mapred.compress.map.output", true);
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
    in_path = new Path(otherArgs[0]);

    //conf.setInt("fuzzyJSplit.universe", threshold);
    
    threshold = Integer.parseInt(otherArgs[2]);
    
    //string_length = Integer.parseInt(otherArgs[4]);
    conf.setInt("fuzzyJSplit.universe", string_length);
   
    
    conf.setInt("fuzzyJSplit.threshold", threshold);
  
  	if(otherArgs[1].equals("null-absolute")){
  		conf.setBoolean("fuzzyJSplit.null_absolute", true);
    }
  
    Job job = new Job(conf, "Fuzzy Join [Splitting Alg., Edit Distance Version: threshold "+threshold+"]");
	job.setNumReduceTasks(Integer.parseInt(otherArgs[3])); 
	//job.setNumReduceTasks(0); 
	job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
    job.setJarByClass(FuzzyJoinSplitEdit.class);
    job.setMapperClass(FuzzyJoinSplitEditMapper.class);
    job.setPartitionerClass(ChunkHashingPartitioner.class);
    //reducer set below
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   	FileInputFormat.addInputPath(job, in_path);
   	
   	if(otherArgs[1].equals("null")){
		System.err.println("Note: ALL OUTPUT DISCARDED");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitEditReducer.class);
	} else if (otherArgs[1].equals("null-cost")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (measuring communication cost)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitNullReducer.class);
	} else if (otherArgs[1].equals("null-absolute")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (completely)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitEditReducer.class);
		//conf.setBoolean("fuzzyJSplit.null_absolute", true); MOVED upwards because of when Job is generated
	} else {
		job.setReducerClass(FuzzyJoinSplitEditReducer.class);
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
