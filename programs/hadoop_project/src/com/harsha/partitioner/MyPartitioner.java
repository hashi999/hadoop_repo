package com.harsha.partitioner;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MyPartitioner implements Partitioner<Text, IntWritable> {
	
	public int getPartition(Text key, IntWritable value, int numPartitions){
		
		String key1 = key.toString().toLowerCase();
		
		if(key1.equals("hadoop"))
			return 0;
		if(key1.equals("learning"))
			return 1;
		
		return 2;
		
	}

	@Override
	public void configure(JobConf arg0) {
	}


}
