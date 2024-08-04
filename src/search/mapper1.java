package search;

/*
 * @author Yash Vaidya
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper1 extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{

        
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub

		String[] parts = value.toString().split("\t");
		
		
		output.collect(new Text(parts[0]), new Text(parts[1]));
			
		
		
	}

}
