

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

import ca.uvic.csc.research.MetadataIntArrayWritable;
//import ca.uvic.csc.research.Text;
import ca.uvic.csc.research.EditDistInt; //edit distance library

import java.lang.Integer;
import java.lang.Long;
import java.lang.String;
//import java.util.HashMap;
import java.util.ArrayList;
//import java.io.File;
//import java.io.PrintWriter;


public class FuzzyJoinNaiveJaccard {
	public static Path in_path;
	public static String in_name;
	public static int granularity;
	public static float threshold;
	
	//NOT YET FOR USE WITH BINARY STRINGS OF LENGTH > 32
	//with 132 reducers the highest granularity we can do is 15, using 120 reducers
	//
	
  	public static class FuzzyJoinNaiveJaccardMapper 
       extends Mapper<IntWritable, IntArrayWritable, ByteArrayWritable, MetadataIntArrayWritable>{
    
	private int granularity_mod; // determines how many buckets the strings are hashed to. G should ideally be a power of 2. The granularity G also specifies the number of reducers: It is always (G(G+1))/2 [so for G=4, you'd have 10 reducers].
    private ByteWritable key1;
    private ByteWritable[] keyOut;
    private MetadataIntArrayWritable valueOut;
    private String string_temp;

    
      
    public void map(IntWritable key, IntArrayWritable value, Context context
                    ) throws IOException, InterruptedException {
     Configuration conf = context.getConfiguration();
	 granularity_mod = conf.getInt("fuzzyJNaiveJaccard.granularity", 1);
	 if(granularity_mod < 1){
	 granularity_mod = 1;
	 }
     
     //String[] line = value.toString().trim().split("\\s",1);
     //expected: 1 value: the original string
	
	  /*if(line[0].length() > 32){
	  	throw new IOException("Error: a binary string is too long.");
	  }*/
      key1 = new ByteWritable((byte)Math.abs(key.get() % granularity_mod)); // parse as binary, modulo appropriately
      //Text valueOut;// = new Text(line[0]);
      keyOut = new ByteWritable[2];
      
      for(int j=0; j<granularity_mod; j++){ //output G copies to G reducers
      	if(key1.get() < j){
      		keyOut[0] = key1;
      		keyOut[1] = new ByteWritable((byte)j);
      		valueOut  = new MetadataIntArrayWritable(value.get(), key.get(), (byte)1);
      		//valueOut.setMeta((byte)1); //A side - you have to specify int.class or it'll get autoboxed to Integer :P
      	} else if (key1.get() > j) {
      		keyOut[0] = new ByteWritable((byte)j);
      		keyOut[1] = key1;
      		valueOut = new MetadataIntArrayWritable(value.get(), key.get(), (byte)2);
      		//valueOut.setMeta((byte)2); //B side
      	} else {
      		keyOut[0] = key1;
      		keyOut[1] = key1;
      		valueOut  = new MetadataIntArrayWritable(value.get(), key.get(), (byte)0);
      		//Integer dir = new Integer(0);
      		//valueOut.setMeta((byte)0); //both sides
      	}
       //System.err.println("Objects written: "+valueOut[0]+" and "+valueOut[1]);
      context.write(new ByteArrayWritable(keyOut), valueOut); //write string and hashed numeric directional value
      }
    }
  }
  
  public static class TriangleHashPartitioner extends Partitioner<ByteArrayWritable, MetadataIntArrayWritable>
	{
	   @Override
 	   public int getPartition(ByteArrayWritable key, MetadataIntArrayWritable value,int numReduceTasks)
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
       extends Reducer<ByteArrayWritable,MetadataIntArrayWritable,Text,Text> {
  	//dummy - reduces nothing. Used for measuring comm. cost
  		public void reduce(ByteArrayWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
                       return;
        }
  	}
  
  public static class FuzzyJoinNaiveJaccardReducer
       extends Reducer<ByteArrayWritable,MetadataIntArrayWritable,Text,Text> {

	private float similarity_threshold; //how close two strings have to be to qualify. Higher = more permissive
	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;
	private int universe_size; //how many items are in the universe that each sorted set draws its collection from
	private static boolean null_absolute;
	private long records_written = 0;

	//final static int[] byte_ones = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4}; // lookup table for hamming distance between two bytes

	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;


	private int calculateJaccard(int s1_length, int s2_length, float jaccard_threshold){
		// now for the interesting part...
					
		//"Let’s say you have two strings of length a and b, where a >= b.
		//Now let’s say they have c overlapping characters (you don’t know this yet).
		//You run edit distance of the two (the ins-del-only variant). You know that there must be (a-b) deletions; then, the edit distance will be (a-b)+2*(b-c), because if there are c overlapping characters, then the (b-c) remaining characters will be ‘deleted’ then ‘inserted’ to synchronize them."
		
		//Put another way: we have two arrays, s1 and s2. The sizes are s1.length and s2.length— assuming s1.length >= s2.length.
		//To make them match, there are (s1.length - s2.length) deletions from s1. Now, we know these sets are SORTED, so suppose there are N specific matching integers in both arrays. This means there are (s2.length - N) NONmatching characters in these arrays. It will take 2*(s2.length - N) deletions and insertions to replace them, because no substitions, remember?
		// So the edit distance we get out of the thing will be equal to: 
		// (s1.length - s2.length) + 2*(s2.length - N)  OR (s1.length - N) + (s2.length - N) OR (s1.length + s2.length) - 2N.
		// Now for the interesting part. We're really looking for N here. That's our intersection. The union is (s1.length + s2.length) - N. [because there are N matching integers and they're in both arrays so we counted them twice, so subtract one]
		// So what's our Jaccard distance? For similarity, it's intersection over union, or:
		// N / ((s1.length + s2.length) - N)
		// Ho-kay, so if we want a Jaccard threshold of greater than J (intersect over union measures similarity, 1-Jdist is dissimilarity), we need to define N in terms of J and also the lengths.
		// I'm going to call (s1.length + s2.length) just S, for sum of lengths.
		// J = N/(S-N)
		// J*(S-N) = N
		// J*S - J*N = N
		// J*S = (J+1)N
		// (J*S)/(J+1) = N
		// so how does this relate to the edit distance? Well, the distance is defined as:
		// 		S - 2N, remember.
		// plugging our formula for N in gives:
		// S - 2((J*S)/(J+1)) = edit distance
		// S - (2*J*S)/(J+1) = edit distance...
		// S* (1 - (2*J)/(J+1)) = edit distance!
									
		int s = s1_length+s2_length;
		float j = 1- (2*jaccard_threshold)/(jaccard_threshold+1);
	
		return (int)(s*j); //cast to int - truncates (rounds down)
		// example: s1 len is 5, s2 len is 3. Jacc sim is .5 (50%). Edit dist threshold is 8*(1-(2*.5)/(1.5)) = 8*(1-1/1.5) = 8*(1-2/3) = 8*(1/3) = 8/3 = 2.666666... Wait, what? OK, we very clearly do not have an integer threshold. What now?
		// look at it this way. Because of the differences in the lengths of the strings, there will always be two edits (deletions from s1).
		// If all other characters in the string are identical, intersection / union is 3/(8-3) = 3/5. .6.
		// If one is different, intersection / union is 2/(8-2) = 2/6 = 1/3. .3. And it only gets worse. The threshold is: at most that many swaps, or nothin'.
	}


    public void reduce(ByteArrayWritable key, Iterable<MetadataIntArrayWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
       //	universe = conf.getInt("fuzzyJNaiveJaccard.universe", 1000); 
		similarity_threshold = conf.getFloat("fuzzyJNaiveJaccard.threshold", 0.5f); 
		null_absolute = conf.getBoolean("fuzzyJNaiveJaccard.null_absolute", false);              
        
        boolean hybrid_mode = true; //starts assuming it's getting in two sets of items and joining just those
                       
     	ArrayList<int[]> nums_list_1 = new ArrayList<int[]>();
     	ArrayList<int[]> nums_list_2 = new ArrayList<int[]>();
     	ArrayList<Integer> tags_list_1 = new ArrayList<Integer>();
     	ArrayList<Integer> tags_list_2 = new ArrayList<Integer>();
     	//System.err.println("reducer: ready to add!");
     	context.setStatus("reduce: reading data");
     	for (MetadataIntArrayWritable val : values) { // no need to keep a dict; the system tosses by key!!
     		//note: later duplicate records will overwrite earlier ones.
     		//int dir = val.toString().charAt(0);
     		int[] nums = val.toIntArray();
     		byte dir = val.getMeta();
     		//text = text.substring(1);
     		
     		if(!hybrid_mode || dir == 0){
     			nums_list_1.add(nums);
     			tags_list_1.add(val.getTag());
     			hybrid_mode = false;
     		} else if (dir == 1) {
     			nums_list_1.add(nums);
     			tags_list_1.add(val.getTag());
     		} else if (dir == 2) {
     			nums_list_2.add(nums);
     			tags_list_2.add(val.getTag());
     		} else {
     			System.err.println("Dir doesn't match!");
     		}
       	}
      
      //that's all data from the reducer read in. Next is processing it.
      	
      	
     	int list_1_size = nums_list_1.size();
        int list_2_size = nums_list_2.size();
     	int[] s1, s2;
     	int t1, t2, t;
     	 
		if(hybrid_mode){ // we're using two lists
		context.setStatus("reduce: joining data [two sets]");
			for(int i=0; i< list_1_size; i++){
				if(!null_absolute || i % 100 == 0){
					context.setStatus("reduce: joining data [two sets] ("+(i+1)+"/"+list_1_size+")");
				}
				for(int j=0; j< list_2_size; j++){
					s1 = nums_list_1.get(i);
					s2 = nums_list_2.get(j);
					
					t = EditDistInt.distanceTest(s1,s2,calculateJaccard(s1.length, s2.length, similarity_threshold),2); //Jaccard distance doesn't do substitutions
					
					//So we have the edit distance and we want it back into jaccard.
					//Jaccard is J = N/(S-N) [intersection / union -- N is intersection, and S-N is union.]
					//S is the sum of the two string lengths.
					//Now, we've already established that the edit distance, E, is S - 2N.
					//Proof: Each time you add a character to, or delete a character from, a string, that counts as 1 operation.
					//So let's say there are X nonmatching characters across the two strings. S-X = 2N because the matching characters come up once.
					//To turn one string into the other, EACH nonmatching character has to EITHER be deleted, OR added once an appropriate deletion has occured. So each nonmatching character has one operation (deletion or addition) performed that pertains to it.
					//There are N characters that are the same in each string (matching characters)
					//So union is S-N = (S-2N)+N = E+N.
					//Intersection is: N = (S-2N)-S+3N = E-S+3N; N = E-S+3N => S-E = 2N => (S-E)/2 = N
					//E = S - 2N  => E - S = -2N => S - E = 2N 
					
					//Now, Mathematica keeps telling me that (S-E)/(S+E) = ((S-E)/2)/(E+N) = N/(S-N). 
					// This one's a fairly easy one to prove if we focus on the first two thirds of the equation: (S-E)/(S+E) = ((S-E)/2)/(E+N).
					// If we multiply both sides of the right half of the above by 2:
					// (S-E)/(S+E) = (S-E)/(2(E+N)). So that's the top half taken care of. We just need to prove 2E + 2N = S+E.
					// Well, we already proved E = S-2N. so 2E + 2N = S+E => 2(S-2N) + 2N = S+E => 
					// 		2S - 4N + 2N = 2S - 2N = S+(S-2N) = S+E.
					// So we have a few choices for our output equation. Of these, (S-E)/(S+E) is the easiest to do.
										
					if(t >= 0){
						if(!null_absolute){
							t1 = tags_list_1.get(i);
							t2 = tags_list_2.get(j);
							int sumlen = s1.length+s2.length;
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
			}
		
		} else { // we're only using one
			context.setStatus("reduce: joining data [one set]");
			for(int i=0; i< list_1_size; i++){
				if(!null_absolute || i % 1000 == 0){
					context.setStatus("reduce: joining data [one set] ("+(i+1)+"/"+list_1_size+")");
				}
				for(int j=i+1; j< list_1_size; j++){
					s1 = nums_list_1.get(i);
					s2 = nums_list_1.get(j);
					t = EditDistInt.distanceTest(s1,s2,calculateJaccard(s1.length, s2.length, similarity_threshold),2); //Jaccard distance doesn't do substitutions
					if(t >= 0){
						if(!null_absolute){
							t1 = tags_list_1.get(i);
							t2 = tags_list_1.get(j);
							int sumlen = s1.length+s2.length;
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
      System.err.println("Usage: FuzzyJoinNaiveJaccard <in> <out> <similarity_threshold> <granularity>; granularity must be less than or equal to n, where n(n+1)/2 is the number of reducers you have. Similarity is in a range from 0.0-1.0. Setting <out> to 'null' discards output; 'null-cost' discards after copying to reducer.");
      System.exit(2);
    }
    //conf.setBoolean("mapred.compress.map.output", true);
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
    in_path = new Path(otherArgs[0]);

    
    /*granularity = Integer.parseInt(otherArgs[4]);
    conf.setInt("fuzzyJNaiveJaccard.universe", universe);*/
    granularity = Integer.parseInt(otherArgs[3]);
    conf.setInt("fuzzyJNaiveJaccard.granularity", granularity);
    threshold = Float.parseFloat(otherArgs[2]);
    conf.setFloat("fuzzyJNaiveJaccard.threshold", threshold);
  
  	if(otherArgs[1].equals("null-absolute")){
  		conf.setBoolean("fuzzyJNaiveJaccard.null_absolute", true);
    }
  
  
    Job job = new Job(conf, "Fuzzy Join [Jaccard Distance, Naive: similarity "+threshold+"]");
	job.setNumReduceTasks((granularity*(granularity+1))/2); 
	//job.setNumReduceTasks(0); 
    job.setJarByClass(FuzzyJoinNaiveJaccard.class);
    job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
    job.setMapperClass(FuzzyJoinNaiveJaccardMapper.class);
    job.setPartitionerClass(TriangleHashPartitioner.class);
    //reducer set below
    job.setMapOutputKeyClass(ByteArrayWritable.class);
    job.setMapOutputValueClass(MetadataIntArrayWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   	FileInputFormat.addInputPath(job, in_path);
   	if(otherArgs[1].equals("null")){
		System.err.println("Note: ALL OUTPUT DISCARDED");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinNaiveJaccardReducer.class);
	} else if (otherArgs[1].equals("null-cost")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (measuring communication cost)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinNaiveNullReducer.class);
	} else if (otherArgs[1].equals("null-absolute")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (completely)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinNaiveJaccardReducer.class);	
	} else {
		job.setReducerClass(FuzzyJoinNaiveJaccardReducer.class);
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
