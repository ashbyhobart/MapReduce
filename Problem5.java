/**
 * Problem5.java
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


public class Problem5 {

    /* 
     * Put your mapper and reducer classes here. 
     * Remember that they should be static nested classes. 
     */
	 
	 
	 public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
		 
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 String line = value.toString();
			 
			 //System.out.println(line);
			 
			 String[] words = line.split(";");
			 String[] fixed = {};
			 
			 String[] pre = line.split(",");
			 String id = pre[0];
			 
			 try{
				 fixed = words[1].split(",");
			 } catch(Exception e){
			 }
			 
			for(String field : fixed){
				context.write(new Text(id), new IntWritable(1));
			}
		 
		}
	 
	 }
	 
	 
	 public static class MyReducer extends
        Reducer<Text, IntWritable, Text, LongWritable> 
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
             throws IOException, InterruptedException 
        {
			
			long cnt = 0;
			
			for(IntWritable val : values){
				cnt += val.get();
			}
	 
			context.write(key, new LongWritable(cnt));
		
		}
		
	}
	 
	 
	public static class MyMapper2 extends Mapper<Object, Text, Text, Text>{
		 
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 String dummyKey = "friendliest";
			 context.write(new Text(dummyKey), value);
		 
		}
	 
	 }
	 
	 
	 public static class MyReducer2 extends
        Reducer<Text, Text, Text, LongWritable> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context) 
             throws IOException, InterruptedException 
        {
			String keyString = "";
			long best = 0;
			String gander;
			String[] ganderize;
			
			long trav;
			
			for(Text val : values){
				gander = val.toString();
				ganderize = gander.split("\t");
				trav = Long.parseLong(ganderize[1]);
				
				if(trav >= best){
					best = trav;
					keyString = ganderize[0];
				}
				
			}
	 
			context.write(new Text(keyString), new LongWritable(best));
		
		}
		
	}
	 
	 
	 
	 

    public static void main(String[] args) throws Exception {
        /*Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 5");
        job.setJarByClass(Problem5.class);

        
        // See Problem3.java for comments describing the calls.

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //   job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);*/
		
		Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "problem 5-1");
        job1.setJarByClass(Problem5.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem3.java for comments describing the calls.

        job1.setMapperClass(MyMapper.class);
        job1.setReducerClass(MyReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        //   job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class); //IntWritable

        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);


        /*
	 * Second job in a chain of two jobs
	 */
        //Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf, "problem 5-2");
        job2.setJarByClass(Problem5.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem3.java for comments describing the calls.

        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        //   job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class); //IntWritable

        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
		
		
		
		
		
		
    }
}
