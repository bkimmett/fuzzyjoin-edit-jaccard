

package ca.uvic.csc.research;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import ca.uvic.csc.research.IntArrayWritable;
import ca.uvic.csc.research.RangeIntArrayWritable;
import ca.uvic.csc.research.MetadataIntArrayWritable;
//import ca.uvic.csc.research.Text;
import ca.uvic.csc.research.EditDistInt; //edit distance library

import java.lang.Integer;
import java.lang.Long;
import java.lang.String;
//import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
//import java.io.File;
//import java.io.PrintWriter;


public class FuzzyJoinSplitJaccard {
	public static Path in_path;
	public static boolean first_string = true;
	private static float similarity;
	
	private static int array_length; 
	

	//NOT YET FOR USE WITH BINARY STRINGS OF LENGTH > 32
	
  	public static class FuzzyJoinSplitJaccardMapper 
       extends Mapper<IntWritable, IntArrayWritable, RangeIntArrayWritable, MetadataIntArrayWritable>{
    
    private IntWritable key1;
    private IntArrayWritable keyOut;
   // private MetadataIntArrayWritable valueOut_ready;// = new MetadataShortWritable();
    private static int[] string_chunks;
	private static int[] shift_positions;
	private static boolean chunks_set = false;
	private static int threshold;
	private static final IntWritable[] stub = new IntWritable[0]; //used for toArray() to get the type. It's too small to actually be allocated to.
	private static int only1, only2;
	private static boolean only = false;
    
      
    public void map(IntWritable key, IntArrayWritable value, Context context
                    ) throws IOException, InterruptedException {
     Configuration conf = context.getConfiguration();
	 
    	MetadataIntArrayWritable valueOut = new MetadataIntArrayWritable(value.get(), key.get(), (byte)0x00); //set up value
		if (!chunks_set) {
			chunks_set = true;
			threshold = conf.getInt("fuzzyJSplit.threshold", 1);
			/*DEBUG CODE - allows tracking a solo pair of items */
			/*only = conf.getBoolean("only", false);
			if(only){
    			only1 = conf.getInt("only1", 0);
    			only2 = conf.getInt("only2", 1);
    		}*/
			//figure out where splits should occur
			
			//string_chunks = new int[threshold+1];
			shift_positions = new int[threshold+1];
			
			for(int i = 0; i<threshold+1; i++){ //retrieve masking array from conf
				//string_chunks[i] = conf.getInt("fuzzyJSplit.stringChunk"+i, -1);
				shift_positions[i] = conf.getInt("fuzzyJSplit.shiftPos"+i, -1);
			}
		}
		
		/*DEBUG CODE - allows tracking a solo pair of items */
		/*if(only && key.get() != only1 && key.get() != only2){
			return;
		}*/
		
		ArrayList<IntWritable> currentBlock = new ArrayList<IntWritable>();
		int currentSplit = 0;
		int currentBoundary = shift_positions[0];
		int lastBoundary = 0;
		//System.err.println("Starting boundary: "+shift_positions[0]);
		
		for(IntWritable item : value.get()){
			//System.err.println("Reading item "+item+".");
			int val = item.get()-1; //bug fix- data is one-indexed
			RangeIntArrayWritable keyOut;
			while(val > currentBoundary || (val == currentBoundary && currentSplit < threshold)){
				
				//System.err.println("Value "+val+" is beyond boundary "+currentBoundary+".");
				
				//this item is outside the current boundary. Pack and send off the current chunk.
				if(currentBlock.size() > 0){
					keyOut = new RangeIntArrayWritable(currentBlock.toArray(stub), lastBoundary, currentBoundary-1);
					context.write(keyOut, valueOut); // write takes place here
				} /*else {
					//keyOut = new RangeIntArrayWritable(new IntWritable[0], lastBoundary, currentBoundary-1);
					//we do NOT write if a block is empty. Because this is Jaccard similarity (ultimately), empty blocks won't help matches any.
				}*/
				
				
				/*System.err.print("Partitioner chunk {{"+keyOut+"}} has hash "+Arrays.hashCode(keyOut.toIntArray())+" and key "+keyOut.getLower()+". ");
				System.err.println("Resulting partition: "+(Math.abs(Arrays.hashCode(keyOut.toIntArray()) ^ keyOut.getLower() ^ keyOut.getUpper()))+".");*/
				currentSplit++;
				/*if(currentSplit >= threshold){
					System.err.println("We're trying to place item "+val+"("+(val+1)+") beyond the last split, ending at "+currentBoundary+".");
				}*/
				lastBoundary = currentBoundary;
				currentBoundary = shift_positions[currentSplit];
				//System.err.println("New boundary: "+shift_positions[currentSplit]);
				currentBlock.clear(); //empty list
			}
			currentBlock.add(item);
		}
		//we may have one more split that isn't covered by that while loop
		if(currentBlock.size() > 0){
			RangeIntArrayWritable lastKeyOut = new RangeIntArrayWritable(currentBlock.toArray(stub), lastBoundary, currentBoundary-1);
			context.write(lastKeyOut, valueOut);
		}
		/*System.err.println("Partitioner chunk {{"+lastKeyOut+"}} has hash "+Arrays.hashCode(lastKeyOut.toIntArray())+" and key "+lastKeyOut.getLower()+".");
 	   	System.err.println("Resulting partition: "+(Math.abs(Arrays.hashCode(lastKeyOut.toIntArray()) ^ lastKeyOut.getLower() ^ lastKeyOut.getUpper()))+".");*/
		     
      }
    }

	
	public static class ChunkHashingPartitioner extends Partitioner<RangeIntArrayWritable, MetadataIntArrayWritable>{
		int window;
	   @Override
 	   public int getPartition(RangeIntArrayWritable key, MetadataIntArrayWritable value, int numReduceTasks)
 	   {	
 	   		window = key.getUpper() - key.getLower() + 1;
 	   		
 	   	   //return Math.abs(Arrays.hashCode(key.toIntArray()) ^ key.getLower() ^ key.getUpper()) % numReduceTasks; // simple modulo hashing for relatively equal distribution. 
 	   	   return (key.getLower() % numReduceTasks + Math.abs(Arrays.hashCode(key.toIntArray())) % window) % numReduceTasks; //first assign based on where the window is in the data set then drop in from there
 	   }  
	}

	public static class FuzzyJoinSplitNullReducer
       extends Reducer<RangeIntArrayWritable,MetadataIntArrayWritable,Text,Text> {
       //returns without reducing anything. Used for measuring communication cost.
		public void reduce(RangeIntArrayWritable key, Iterable<IntArrayWritable> values, 
		   Context context
		   ) throws IOException, InterruptedException {
	   
		  	 return;
		   }
  
  }
  
  
  public static class FuzzyJoinSplitJaccardReducer
       extends Reducer<RangeIntArrayWritable,MetadataIntArrayWritable,Text,Text> {
       
       
    //private static int[] string_chunks;
	private static int[] shift_positions;
	//private static int shift_split;
	//private static int shift_size;
	private static boolean chunks_set = false;
	private static int threshold;
	private static float similarity;
	private static boolean null_absolute;
	private long records_written = 0;

	private int calculateJaccard(int s1_length, int s2_length, float jaccard_threshold){
		// see the naive jaccard file for an explanation of why this works.		
		int s = s1_length+s2_length;
		float j = 1- (2*jaccard_threshold)/(jaccard_threshold+1);
	
		return (int)(s*j); //cast to int - truncates (rounds down)
	}


	private boolean substring_verify(int[] one, int[] two, int checkBefore){ //check that all preceding chunks of the strings are not equal
		//this takes O(n) time now, at least theoretically. But in practice, it's fast.
		if(checkBefore == 0){ /*System.err.println("succeeded. (trivial)");*/ return true; } // trivial case; we're good to go
		int place1 = 0;
		int place2 = 0;
		int boundary_index = 0;
		int boundary = shift_positions[boundary_index];
		boolean found_no_mismatches_in_boundary = true;
		boolean found_items_in_boundary = false;
		int item1 = one[place1];
		int item2 = two[place2];
		//System.err.println("Items start at "+item1+" and "+item2+".");
			while(true){
				while(item1 > boundary && item2 > boundary){ //reset boundary - > instead of >= because items are one-indexed. BUG FIX
					if(found_no_mismatches_in_boundary && found_items_in_boundary){ 
						/*if(boundary_index > 0){
							System.err.println("SV failed at ["+(shift_positions[boundary_index-1]+1)+":"+boundary+"].");
						} else {
							System.err.println("SV failed at [0:"+boundary+"].");
						}*/
					
					 return false; } //we jumped a boundary segment without finding any mismatches! Wut-oh.
					found_no_mismatches_in_boundary = true;
					found_items_in_boundary = false;
					boundary = shift_positions[++boundary_index];
					if(boundary > checkBefore){ 
					 	/*if(boundary_index > 1){
							System.err.println("succeeded at ["+(shift_positions[boundary_index-2]+1)+":"+shift_positions[boundary_index-1]+"].");
						} else {
							System.err.println("succeeded at [0:"+shift_positions[boundary_index-1]+"].");
						}*/
						return true; 
					} //we've made it; last segment was OK
				}
				
				if(item1 < item2){
					found_items_in_boundary = true;
					found_no_mismatches_in_boundary = false;
					item1 = one[++place1]; //increment place1 then store item1 again
					//System.err.println("Updating item 1 to "+item1+".");
				} else if(item1 > item2){
					found_items_in_boundary = true;
					found_no_mismatches_in_boundary = false;
					item2 = two[++place2]; //increment place2 then store item2 again
					//System.err.println("Updating item 2 to "+item2+".");
				} else if(item1 == item2){
					found_items_in_boundary = true;
					item1 = one[++place1];
					item2 = two[++place2];
					//System.err.println("Match. Moving to "+item1+" and "+item2+"."); 
				}
				
			}		
	}

    public void reduce(RangeIntArrayWritable key, Iterable<MetadataIntArrayWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        //System.err.println("Reducer's ready.");
        null_absolute = conf.getBoolean("fuzzyJSplit.null_absolute", false);
        if (!chunks_set) {
			chunks_set = true;
			threshold = conf.getInt("fuzzyJSplit.threshold", 1);
			similarity = conf.getFloat("fuzzyJSplit.similarity", (float)0.5);
			//shift_size = conf.getInt("fuzzyJSplit.shiftSize", 5);
			//shift_split = conf.setInt("fuzzyJSplit.shiftSplit", 0);
			
			/*if(line[0].length() > 32){
				throw new IOException("Error: a binary string is too long.");
			} */
			//figure out where splits should occur
			
			//string_chunks = new int[threshold+1];
			shift_positions = new int[threshold+1];
			
			for(int i = 0; i<threshold+1; i++){ //retrieve masking array from conf
				//string_chunks[i] = conf.getInt("fuzzyJSplit.stringChunk"+i, -1);
				shift_positions[i] = conf.getInt("fuzzyJSplit.shiftPos"+i, -1);
			}
			
		}
        
     	ArrayList<int[]> int_list = new ArrayList<int[]>();
     	ArrayList<Integer> tag_list = new ArrayList<Integer>();
     	//System.err.println("reducer: ready to add!");
     	context.setStatus("reduce: reading data");
     	//System.err.println("Key: "+key+" ["+key.getLower()+":"+key.getUpper()+"]");
     	for (MetadataIntArrayWritable val : values) { // no need to keep a dict; the system tosses by key!!
     		//note: later duplicate records will overwrite earlier ones.
     		int_list.add(val.toIntArray());
     		tag_list.add(val.getTag());
     		//System.err.println("Value: "+val.get());

       	}
      
      //that's all data from the reducer read in. Next is processing it.
      	
      	
     	 int list_size = int_list.size();
     	 
     	 int[] list_1, list_2;
     	 int t1, t2, t;
     	 
     	context.setStatus("reduce: joining data");
		for(int i=0; i< list_size; i++){
			if(!null_absolute || i % 1000 == 0){
				context.setStatus("reduce: joining data ("+(i+1)+"/"+list_size+")");
			}
			for(int j=i+1; j< list_size; j++){
				list_1 = int_list.get(i);
				list_2 = int_list.get(j);
			
				t = EditDistInt.distanceTest(list_1,list_2,calculateJaccard(list_1.length, list_2.length, similarity),2); //Jaccard distance doesn't do substitutions
			
				if(t >= 0){
					t1 = tag_list.get(i);
					t2 = tag_list.get(j);
					//System.err.print("About to check tags "+t1+"::"+t2+" ("+i+":"+j+") on "+key.getLower()+"... ");
					if(substring_verify(list_1, list_2, key.getLower())){
					if(!null_absolute){
						//t1 = tag_list.get(i);
						//t2 = tag_list.get(j);
						//System.err.println("Tag pair: "+t1+"::"+t2+" ("+i+":"+j+") on "+key.getLower());	 
						int sumlen = list_1.length+list_2.length;
						float jac_sim = (float)(sumlen - t) / (float)(sumlen + t);
						if(t1 < t2){
							context.write(new Text(t1+""), new Text(t2+"  "+jac_sim)); // perform comparison. If a success, kick out the pair
						} else {
							context.write(new Text(t2+""), new Text(t1+"  "+jac_sim)); // perform comparison. If a success, kick out the pair
						}
					} else {
						records_written++;
					}
					}
				}
			
				/*if(compareDistance(int_list.get(i),int_list.get(j)) <= threshold && substring_verify(int_list.get(i),int_list.get(j),(int)key.getMeta())){ // if the distance checks out and the strings are lexicographically first...
					if(!null_absolute){
						context.write(new Text(String.format("%32s", Integer.toBinaryString(int_list.get(i))).replace(" ", "0")), new Text(String.format("%32s", Integer.toBinaryString(int_list.get(j))).replace(" ", "0")));
						//context.write(new Text(int_list.get(i)), new Text(int_list.get(j))); // perform comparison. If a success, kick out the pair
					} else {
						records_written++;
					}
				}*/
			/*current_chunk++;
			partial_chunks++;
			context.setStatus("reduce: processing chunk "+current_chunk+"/"+list_size+" ("+partial_chunks+"/"+total_chunks+" total)");*/
			}
		}
		
      }
   }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length <5) {
      System.err.println("Usage: FuzzyJoinSplitJaccard <in> <out> <similarity threshold> <number of reducers> <universe_size>;  Setting <out> to 'null' discards output; 'null-cost' discards after copying to reducer.");
      System.exit(2);
    }
    /*DEBUG CODE - allows tracking a solo pair of items */
    /*if(otherArgs.length == 7){
    	conf.setInt("only1", Integer.parseInt(otherArgs[5]));
    	conf.setInt("only2", Integer.parseInt(otherArgs[6]));
    	conf.setBoolean("only", true);
    } else {
    	conf.setBoolean("only", false);
    }*/
    
    //conf.setBoolean("mapred.compress.map.output", true);
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
    in_path = new Path(otherArgs[0]);

    //conf.setInt("fuzzyJSplit.universe", threshold);
    
    similarity = Float.parseFloat(otherArgs[2]);
    
    array_length = Integer.parseInt(otherArgs[4]);
    conf.setInt("fuzzyJSplit.universe", array_length);
   
    
    //figure out where splits should occur
    
    if(similarity <= 1.0/3.0){
    	 System.err.println("Due to how this algorithm works, it cannot find pairs of strings when the Jaccard similarity is less than or equal to 1/3. Please increase your threshold of similarity or try another algorithm.");
    	  System.exit(2);
    }
    
    int s = 2*array_length;
	float j = 1- (2*similarity)/(similarity+1);
    int threshold = (int)(s*j);
    
   
	//OK, how much this threshold should be is actually kind of a pickle, because the jaccard-to-edit-distance formula works on HOW MANY ITEMS EACH ACTUAL STRING HAS IN IT. So what we're calculating here is actually a 'permissive threshold' that is the maximum needed for any theoretically matching pair of strings with the desired similarity to be kicked out.
	//Note that this doesn't really change the calculation at all. It's still the same formula (S* (1 - (2*J)/(J+1)) = edit distance) as in Naive, except here S is double the universe size. 
	//OK, spotted the problem yet? Once you get below a certain J threshold, you get more splits than there are items in the original sets! The 'hot' J value is actually 1/3:
	// S* (1 - (2*J)/(J+1)) = E (edit distance), but E must be < the size of the largest possible set, which I'll call Sl. (The reason for this is that splits can be empty, and that's not a problem-- the splitting algorithm always sets up the size of its splits by looking at the set with all items in it, Sl).
	// Now, we already said that S = Sl*2 right now, which results in:
	// (2*Sl)* (1 - (2*J)/(J+1)) = E < Sl
	// (2*Sl)* (1 - (2*J)/(J+1)) < Sl
	// 2* (1 - (2*J)/(J+1)) < 1
	// 1 - (2*J)/(J+1) < 1/2
	// 1 < 1/2 + (2*J)/(J+1)
	// 1/2 < (2*J)/(J+1)
	// 1/4 < J/(J+1)
	// 1/4(J+1) < J
	// 1/4J + 1/4 < J
	// 1/4 < 3/4J
	// 1 < 3J
	// 1/3 < J
	// So if you submit a J <= 1/3, this algorithm will politely refuse!
	
			
	//string_chunks = new int[threshold+1];
	//shift_positions = new int[threshold+1];
	
	
	//OK, so... previously, we used this 'threshold' to determine how many splits to do and how big they were. But there's a catch. This time, we're going for Jaccard similarity!!! So previously, there was no possible drawback to adding 1 to the chunk size.
	// Here, adding 1 to the chunk size enables there to be a chunk with sufficient 'matchiness' that the resulting string could fit the similarity, but not be recognized if that chunk was (say) the only chunk that had any items in both strings.
	// Instead, the chunk size we generate is deemed to be the maximum chunk size. If it doesn't divide evenly, we figure out how many that-size-minus-one chunks we can generate to fit.
	
	
	
	int string_chunk_len = array_length / (threshold+1);
	int big_chunks_expected = array_length / string_chunk_len;
	int string_chunk_remain = array_length % string_chunk_len; // this is how much of a chunk is left over. Length-1 segments must be pulled from other chunks to fill out this chunk.
	int leftover_chunks = 0;
	if(string_chunk_remain > 0){
		leftover_chunks = 1+((string_chunk_len-1)-string_chunk_remain);
		big_chunks_expected = (big_chunks_expected - leftover_chunks) + 1;
	}
	
	//now we redefine threshold to how many splits we ACTUALLY HAVE.
	threshold = (big_chunks_expected + leftover_chunks) - 1;
	
	 System.err.println("Threshold is "+(threshold+1)+" ["+string_chunk_len+"*"+big_chunks_expected+","+(string_chunk_len-1)+"*"+leftover_chunks+"].");
	
	//int places_left_to_shift = array_length;
	int boundary = 0;
	
	for(int i = 0; i<threshold+1; i++){
		boundary += (string_chunk_len - ((i<big_chunks_expected)?0:1));
		conf.setInt("fuzzyJSplit.shiftPos"+i, boundary);
		//System.err.println("shiftPos "+i+": "+boundary+".");
	}
			
    
    
    conf.setInt("fuzzyJSplit.threshold", threshold);
    conf.setFloat("fuzzyJSplit.similarity", similarity);
  
  	if(otherArgs[1].equals("null-absolute")){
  		conf.setBoolean("fuzzyJSplit.null_absolute", true);
    }
  
    Job job = new Job(conf, "Fuzzy Join [Splitting Alg., Jaccard Version: similarity "+similarity+", "+array_length+"-item universe]");
	job.setNumReduceTasks(Integer.parseInt(otherArgs[3])); 
	//job.setNumReduceTasks(0); 
	job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
    job.setJarByClass(FuzzyJoinSplitJaccard.class);
    job.setMapperClass(FuzzyJoinSplitJaccardMapper.class);
    job.setPartitionerClass(ChunkHashingPartitioner.class);
    //reducer set below
    job.setMapOutputKeyClass(RangeIntArrayWritable.class);
    job.setMapOutputValueClass(MetadataIntArrayWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   	FileInputFormat.addInputPath(job, in_path);
   	
   	if(otherArgs[1].equals("null")){
		System.err.println("Note: ALL OUTPUT DISCARDED");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitJaccardReducer.class);
	} else if (otherArgs[1].equals("null-cost")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (measuring communication cost)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitNullReducer.class);
	} else if (otherArgs[1].equals("null-absolute")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (completely)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitJaccardReducer.class);
		//conf.setBoolean("fuzzyJSplit.null_absolute", true); MOVED upwards because of when Job is generated
	} else {
		job.setReducerClass(FuzzyJoinSplitJaccardReducer.class);
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
