package search;

/*
 * @author Yash Vaidya
 */

import java.util.ArrayList;
import java.util.Scanner;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class searchDriver extends Configured implements Tool{
        private static String custId;
        private static String dataPath;
	public int run(String[] args) throws Exception{
		
                
                ;
                //Scanner sc = new Scanner(System.in);
                //custId = sc.next();
		//job1 specifications
                //for(String i: args)
                //    System.out.println(i);
                
		JobConf job1 = new JobConf(getConf(),searchDriver.class);
		job1.setJobName("RecSystemJob1");
		job1.setStrings("custId", args);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setJarByClass(searchDriver.class);
		
		job1.setMapperClass(mapper1.class);
		job1.setReducerClass(reducer1.class);
				
		System.out.println("Job1");
		FileInputFormat.addInputPath(job1, new Path(dataPath + "Similarity")); //new Path(args[0]));
                FileOutputFormat.setOutputPath(job1, new Path(dataPath + "User"+custId+"Similarity")); //new Path(args[1]));
                JobClient.runJob(job1);
		
		return 0;
	}
	
	public static void main(String[] args, String custid, String path) throws Exception{
                custId = custid;
                dataPath = path;
		int res = ToolRunner.run(new Configuration(), new searchDriver(), args);		
		
	}

}
