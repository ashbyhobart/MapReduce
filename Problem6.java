/**
 * Problem6.java
 */

import java.io.IOException;
import java.time.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/* 
 * interfaces and classes for Hadoop data types that you may 
 * need for some or all of the problems from PS 4
 */
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Problem6 {
    /*
     * One possible type for the values that the mapper should output
     */
    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }
        
        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }

        public int[] toIntArray() {
            Writable[] w = this.get();
            int[] a = new int[w.length];
            for (int i = 0; i < a.length; ++i) {
                a[i] = Integer.parseInt(w[i].toString());
            }
            return a;
        }
    }

    /* 
     * Put your mapper and reducer classes here. 
     * Remember that they should be static nested classes. 
     */
	 
	 
	 
	 public static class MyMapper extends Mapper<Object, Text, Text, IntArrayWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
	 
			 String[] preFriends = line.split(";"); //Separate and get friends as a string
			 String[] friends = {}; //Get the list the friends
			 
			 String[] preID = line.split(",");
			 String id = preID[0]; //The id
			 int intID = Integer.parseInt(id);
			 int intFriend = -1;
			 
			 try{
				 friends = preFriends[1].split(","); //String list of friend IDs
			 } catch(Exception e){
			 }
			 
			 //Create an array of IntWritables the size of the list of friends
			 IntWritable[] iwFriends = new IntWritable[friends.length];
			 
			 for(int i = 0; i < friends.length; i++){
				 iwFriends[i] = new IntWritable(Integer.parseInt(friends[i]));
			 }
			 
			 IntArrayWritable friendIDs = new IntArrayWritable(iwFriends);
			 
			 String currKey = "";
			 
			 if(friends.length > 0){
				 
				for(String friend : friends){
					intFriend = Integer.parseInt(friend);
					
					if(intFriend > intID){
						currKey = intID + "," + intFriend;
					} else{
						currKey = intFriend + "," + intID;
					}
					context.write(new Text(currKey), friendIDs);
				}
			
			 }
	 
		}
	 
	 }
	 
	 
	 public static class MyReducer extends Reducer<Text, IntArrayWritable, Text, Text>{ 
	 
	 
		public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) 
             throws IOException, InterruptedException 
        {
			
			int[] s1 = {}, s2 = {};
			long cnt = 0;
			
			for(IntArrayWritable val : values){
				
				if(cnt == 0){
					s1 = val.toIntArray();
				} else if(cnt == 1){
					s2 = val.toIntArray();
				} else{
					System.out.println("You shouldn't be here");
				}
					
				cnt++;
			}
			
			Set<Integer> first = new HashSet<Integer>();
			Set<Integer> second = new HashSet<Integer>();
			
			for(int k : s1){
				first.add(k);
			}
			
			for(int h : s2){
				second.add(h);
			}
			
			first.retainAll(second);
			
			String stringed = first.toString();
			stringed = stringed.substring(1, stringed.length() - 1);
			String[] cleaner = stringed.split(" ");
			String builder = "";
			
			for(String f : cleaner){
				builder += f;
			}
			
			context.write(key, new Text(builder));
			
		
		}
	 
	 
	 
	 
	 
	 
	 
	 
	 }
	 
	 
	 
	 
	 
	 
	 

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 6");
        job.setJarByClass(Problem6.class);


        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem3.java for comments describing the calls.

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
