/*
 * @author Yash Vaidya
 */


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class reducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
        
	@Override
	public void reduce(Text key, Iterator<Text> value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		//System.out.println("In reducer 2");
		double mulSum=0;
		double sqSum1 = 0,sqSum2 = 0;
		String[] ratings = null;
		
		//calculate the similarity
		while(value.hasNext()){
                        String x = value.next().toString();
                        //System.out.println(x);
			ratings = x.split(",");
                        // To test NaN value at key 4,71
                        //if(key.toString().equals("4,71"))
                        //    System.out.println(x);
                         
			mulSum = mulSum + Double.parseDouble(ratings[0]) * Double.parseDouble(ratings[1]);

			sqSum1 = sqSum1 + Double.parseDouble(ratings[0]) * Double.parseDouble(ratings[0]);
			sqSum2 = sqSum2 + Double.parseDouble(ratings[1]) * Double.parseDouble(ratings[1]);
                        
		}
		
		double similarity = 0.0;
	
		//Cosine Similarity calculation
		similarity = mulSum / (Math.sqrt(sqSum1) * Math.sqrt(sqSum2) + 1e-12);
                
                // One variable Linear Regression
                //similarity = (mulSum/(sqSum1 + 1e-12) + mulSum/(sqSum2+1e-12))/2;
                
                //if(key.toString().equals("4,71"))
                //    System.out.println(similarity);
		//System.out.println(similarity);
		output.collect(key, new Text(String.valueOf(similarity)));
		
	}


}
