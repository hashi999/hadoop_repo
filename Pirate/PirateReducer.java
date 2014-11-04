import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;


public class PirateReducer extends  Reducer<Text,IntWritable,Text,IntWritable>{
	
		public void reduce(Text key,Iterator<IntWritable>values,OutputCollector<Text,IntWritable>output,Reporter reporter) throws IOException,InterruptedException {
			
			int sum = 0;
			
			while(values.hasNext()) {
				
				sum+=values.next().get();
			}
			output.collect(key, new IntWritable(sum));
			
		}

}
