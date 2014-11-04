package com.harsha.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MyMap 
	extends MapReduceBase 
	implements Mapper <LongWritable, Text, Text, IntWritable>{
	
	// Input Key and Value are provided by the InputFormat class - Default - TextInputFormat class
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException{
		
		String line = value.toString();
		System.out.println("Line - "+line+"\t byte offset - "+key);
		StringTokenizer words = new StringTokenizer(line);
		while(words.hasMoreElements()){
			value.set(words.nextToken());
			output.collect(value, new IntWritable(1));
		}
		
	}

}