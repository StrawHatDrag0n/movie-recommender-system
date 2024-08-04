package search;

/*
 * @author Yash Vaidya
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class reducer1 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

        private ArrayList<String> search;
        
        public void configure(JobConf job){
            search = new ArrayList<String>(Arrays.asList(job.getStrings("custId")));
            //System.out.println(search.get(0));
        }
    
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            if(key.toString().equals("1682,10"))
                System.out.println(search.contains(key.toString().replace(",",":")));
            
            if(search.contains(key.toString().replace(",", ":")))
                output.collect(key, new Text(values.next().toString()));
                
        }

	

}
