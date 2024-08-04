/*
 * @author Yash Vaidya
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import search.*;

public class MovieRecommender {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		int k = 0, customerId = 0;
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000/"),conf);
		String dataPath = "/user/hduser/cluster6s3/ml-latest-small-1000/";
                if(args.length != 0)
                    dataPath = args[0];
		// Get all the customer provided movie and ratings list from file
		// 'CIdAndMratings'

		FileReader movieFile = null;
		BufferedReader bufferedReader = null;
		String line = "";
		String[] split = null;
		HashMap<String, String> custMovieRatings = new HashMap<String, String>();

		try {

			//movieFile = fs.open(new Path("/CIdAndMratings/part-00000"));
			bufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(dataPath + "CIdAndMratings/part-00000"))));

			while ((line = bufferedReader.readLine()) != null) {
				split = line.split("\t");
				custMovieRatings.put(split[0], split[1]);
			}

		} catch (FileNotFoundException e) {

			e.printStackTrace();

		} catch (IOException e) {

			e.printStackTrace();

		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					
					e.printStackTrace();
				}
			}
			if (movieFile != null) {
				try {
					movieFile.close();
				} catch (IOException e) {
					
					e.printStackTrace();
				}
			}
		}
                
                // Get the input K value(1-10) and customer id(1-650) from user
		Scanner input = new Scanner(System.in);

		// To get a valid k value
		do {
			try {
				System.out
						.println("Please enter the number of top movies to print(upto 10): ");
				k = input.nextInt();
				if (k > 100 || k == 0) {
					System.out.println("Error: Enter a number from 1 to 10");
				}

			} catch (InputMismatchException e) {
				System.out.println("Error: Enter a valid number upto 10");
			}
			input.nextLine();

		} while (k < 1 || k > 100);

		// To get a valid Customer Id value

		do {
			try {

				System.out.println("Please enter Customer Id(between 1 and 650): ");
				customerId = input.nextInt();

				if (customerId > 943 || customerId == 0) {
					System.out.println("Error: Enter a Customer Id between 1 to 650");
				}

			} catch (InputMismatchException e) {
				System.out.println("Error: Enter a valid Customer Id ");
			}

			input.nextLine();

		} while (customerId > 650 || customerId < 1 || custMovieRatings.get(String.valueOf(customerId)) == null);

		input.close();

		

		// Find movies that were not rated for the customer Id

		HashMap<String, String> ratedMovies = new HashMap<String, String>();
		String custMovieRating = custMovieRatings.get(String
				.valueOf(customerId));
    
		String[] splitMRating = custMovieRating.split(" ");
		String[] splitMR = null;

		for (int i = 0; i < splitMRating.length; i++) {

			splitMR = splitMRating[i].split(",");
			ratedMovies.put(splitMR[0], splitMR[1]);

		}

		ArrayList<Integer> notRated = new ArrayList<Integer>();

		for (int j = 1; j <= 1000; j++) {
			if (ratedMovies.get(String.valueOf(j)) == null) {
				notRated.add(j);
			}
		}

		// Calculate the Prediction for movies that were not rated by customer
		double simValue = 0.0, p1 = 0.0, p2 = 0.0, prediction = 0.0;
		HashMap<Integer, Double> moviePrediction = new HashMap<Integer, Double>();

                ArrayList<String> searchList = new ArrayList<String>();
                
                for (int value: notRated){
                    for(String rated: ratedMovies.keySet()){
                        searchList.add(String.valueOf(value)+":"+rated);
                    }
                }
                
                //for(String i: searchList)
                //    System.out.println(i);
                
                String [] arr = searchList.toArray(new String[searchList.size()]); 
                searchDriver.main(arr,String.valueOf(customerId), dataPath);
                
                
                HashMap<String,String> userSimilarity = new HashMap<String,String>();
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(dataPath+"User"+String.valueOf(customerId)+"Similarity/part-00000"))));
                String line1;
                while((line1=reader.readLine()) != null){
                    String[] parts = line1.split("\t");
                    userSimilarity.put(parts[0], parts[1]);
                }
                
		for (int value : notRated) {

			for (String rated : ratedMovies.keySet()) {

				simValue = 0;
                                String temp = userSimilarity.get(String.valueOf(value)+","+rated);
                                if(temp != null)
                                    simValue = Double.valueOf(temp);				
                                p1 = p1 + simValue * Double.valueOf(ratedMovies.get(rated));
				p2 = p2 + simValue;

			}

			if (p1 != 0.0) {
				prediction = p1 / (p2 + 1e-12);
			}

			// Putting the non-rated movie and its prediction in map
			moviePrediction.put(value, prediction);

		}

		// Sort the Predictions and give the top k movies for customer Id

		List<Map.Entry<Integer, Double>> movPred = new LinkedList<Map.Entry<Integer, Double>>(
				moviePrediction.entrySet());
		Collections.sort(movPred, new Comparator<Map.Entry<Integer, Double>>() {

			public int compare(Map.Entry<Integer, Double> map1,
					Map.Entry<Integer, Double> map2) {

				return (map2.getValue()).compareTo(map1.getValue());
			}

		});

		// Printing the first k movies 
	
		int counter = 1;
		
		System.out.println("\n The top "+ k +" movies that are recommended for the customer "+ customerId+" are: \n");
	
		for(Iterator<Map.Entry<Integer, Double>> iterator = movPred.iterator();iterator.hasNext();) {
		 
		  Map.Entry<Integer, Double> entry = iterator.next();
		  
		  System.out.println("Movie # "+entry.getKey()+ " "+ entry.getValue());
		  
		  if(counter == k){
			  break; 
		  }
		  
		  counter++;
		 }
		 

		
	}
}
