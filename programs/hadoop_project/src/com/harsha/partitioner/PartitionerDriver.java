package com.harsha.partitioner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PartitionerDriver {

	public static void main(String[] args) throws Exception{
		JobConf job = new JobConf(PartitionerDriver.class);
		job.setJobName("partitioner");
		
		job.setNumReduceTasks(3);
		
		job.setMapperClass(MyMap.class);
		job.setCombinerClass(MyReduce.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		

	}

}
