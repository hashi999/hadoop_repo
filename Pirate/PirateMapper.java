import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;



public class PirateMapper extends Mapper <LongWritable,Text,Text,IntWritable> {
	
	
	
		public void Map(LongWritable key, Text value, OutputCollector<Text,IntWritable>output, Reporter reporter) throws IOException,InterruptedException{
			
			String line = value.toString();
			StringTokenizer str = new StringTokenizer(line, " ");
			
				while(str.hasMoreTokens()){
					
					 value.set(str.nextToken());
					 output.collect(value, new IntWritable(1));
					
				}
	}

}
