//import java.io.*;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
	    Job job = Job.getInstance(conf, "user average rating");
	    job.setJarByClass(Driver.class);
	    job.setMapperClass(UserToRatingMapper.class);
	    job.setCombinerClass(UserRatingReducer.class);
	    job.setReducerClass(UserRatingReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
