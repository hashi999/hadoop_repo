package com.harsha.partitioner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MyMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
			throws IOException{
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);
		
		while(st.hasMoreTokens()){
			//key.set(st.nextToken());
			value.set(st.nextToken());
			output.collect(value, new IntWritable(1));
			
		}
		
	}

}
