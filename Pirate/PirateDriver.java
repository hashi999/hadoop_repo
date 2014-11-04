import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class PirateDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		JobConf jobConf = new JobConf(PirateDriver.class);
		//job.setJarByClass(PirateDriver.class);
		jobConf.setJobName("pirate");
		
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(IntWritable.class);
		
		jobConf.setMapperClass(PirateMapper.class);
		jobConf.setReducerClass(PirateReducer.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
	/*	job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);*/
		
		
		
		


	}

}
