/*
 * @author Yash Vaidya
 */

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class RecSystemDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception{
		
		//job1 specifications
                String path = "hdfs://master:9000/";
                if(args.length != 0)
                    path += args[0];
                else
                    path += "/user/hduser/cluster6s3/ml-latest-small-1000";
		JobConf job1 = new JobConf(getConf(),RecSystemDriver.class);
		job1.setJobName("RecSystemJob1");
		
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setJarByClass(RecSystemDriver.class);
		
		job1.setMapperClass(mapper1.class);
		job1.setReducerClass(reducer1.class);
		
		       
		//job2 specifications
		JobConf job2 = new JobConf(getConf(), RecSystemDriver.class);
		
		job2.setJobName("RecSystemJob2");
		
		job2.setInputFormat(KeyValueTextInputFormat.class);
		
		job2.setOutputKeyClass(mapper1.class);
		job2.setOutputValueClass(reducer1.class);

	
        
		job2.setJarByClass(RecSystemDriver.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setJarByClass(RecSystemDriver.class);
		
		job2.setMapperClass(mapper2.class);
		job2.setReducerClass(reducer2.class);
		System.out.println("Job1");
		FileInputFormat.addInputPath(job1, new Path(path + "/Ratings")); //new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(path + "/CIdAndMratings")); //new Path(args[1]));
        JobClient.runJob(job1);
		System.out.println("Job2");
		FileInputFormat.addInputPath(job2, new Path(path + "/CIdAndMratings")); //new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(path + "/Similarity")); //new Path(args[2]));
		JobClient.runJob(job2);		
        
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
	
		int res = ToolRunner.run(new Configuration(), new RecSystemDriver(), args);		
		System.exit(res);
	}

}
