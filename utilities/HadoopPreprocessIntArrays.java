import java.io.*;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import ca.uvic.csc.research.IntArrayWritable;

//White, Tom (2012-05-10). Hadoop: The Definitive Guide (Kindle Locations 5375-5384). OReilly Media - A. Kindle Edition. 


//INPUT ASSUMPTIONS: 
// Your items start at one and are numbered from 1 to whatever without ANY gaps. (So you don't have a 10000-item set that ends at 


public class HadoopPreprocessIntArrays { 
    public static void main( String[] args) throws IOException { 
    	if (args.length !=2) {
     		System.err.println("Usage: HadoopPreprocessIntArrays <input filename> <output filename>");
      		System.exit(2);
    	}
    	HashMap<Integer,ArrayList<IntWritable>> usersets = new HashMap<Integer,ArrayList<IntWritable>>();
    	TreeSet<Integer> itemL = new TreeSet<Integer>();
    	File file = new File(args[0]);
		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader(file));
			String text = null;
			String[] line = null;
			while ((text = reader.readLine()) != null) {
				line = text.trim().split("\\s",3);
				int userid = Integer.parseInt(line[0]);
				int itemid = Integer.parseInt(line[1]);
				if(!usersets.containsKey(userid)){
					usersets.put(userid,new ArrayList<IntWritable>(20));
				}
				usersets.get(userid).add(new IntWritable(itemid));
				itemL.add(itemid);
			}
    	} catch (FileNotFoundException e) {
    		System.err.println("File not found.");
      		System.exit(2);
    	} catch (IOException e) {
  			System.err.println("Error reading file. Continuing with what data I have.");
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
			}
		}
		//reading's done. next, we sort.
		HashMap<Integer, Integer> item_mappings = new HashMap<Integer, Integer>((int)((float)itemL.size()/.75)+1);
		int curItem = 1;
		System.out.println("Mappings: ");
		for( Integer item : itemL){
			item_mappings.put(item, curItem);
			System.out.println(item+":"+curItem);
			curItem++;
			
		}
		for( ArrayList<IntWritable> set : usersets.values()){
			Collections.sort(set);
			for(IntWritable item : set){
				item.set(item_mappings.get(item.get()));
			}
		}
		
		//Now let's write!
    	
        String uri = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create( uri), conf);
        Path path = new Path( uri);
        IntWritable key = new IntWritable();
        IntArrayWritable value = new IntArrayWritable();
        IntWritable[] stub = new IntWritable[19]; //used for toArray() to get the type. It's too small to actually be allocated to.
        SequenceFile.Writer writer = null;
        try { 
            writer = SequenceFile.createWriter( fs, conf, path, key.getClass(), value.getClass());
           
           	for( Integer user : usersets.keySet()){
           		System.out.print("User "+user+": ");
           		String listString = "";
				 int items = 0;
				for (IntWritable i : usersets.get(user))
				{
					listString += i.get() + ", ";
					items++;
				}
				
				System.out.println(listString+"end - "+items+" items");
           		
           		value = new IntArrayWritable(usersets.get(user).toArray(stub));
           		key = new IntWritable(user);
           		writer.append( key, value);
           	}
       		System.err.println("Done.");
      } finally { 
        	IOUtils.closeStream( writer); 
       } 
    } 
}

