package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import main.java.format.CSVInputFormat;

/**
 * Main class for the program
 *
 * @version 1.0
 */
public class App
{
	private static final double deltaConverged = 0.01;
	
	/**
	 * Print the usage in the stdout
	 * 
	 * @param args
	 * 		Arguments pass to the program
	 */
	public static void usage(String[] args){
		System.out.println("Usage: App inputPath outputPath k c");
		System.out.println("\tinputPath: Filepath of the input csv file");
		System.out.println("\toutputPath: Filepath of the output csv file");
		System.out.println("\tk: Number of clusters to use");
		System.out.println("\tc: Column in the file to use");
	}
	
	/**
	 * Main driver of the hadoop task
	 * 
	 * @throws Exception
	 * 
	 * @param args
	 * 		Arguments pass to the program
	 */
	public static void main( String[] args ) throws Exception {
		///// Parse arguments
		if (args.length < 4)
		{
			usage(args);
			return;
		}
		
		String inputPath = args[0];
		String initialOutput = args[1];
		String outputPath;
		int clusterNumber = 0;
		int columnNumber = 0;
		int nbIteration = 0;
		boolean jobDone = false;
		Job job = null;
		Double centers[];
		
		try {
			clusterNumber = Integer.parseInt(args[2]);
		}
		catch(NumberFormatException ex) {
			System.out.println("[Error] Can't parse cluster number !");
			usage(args);
			return;
		}
		
		try {
			columnNumber = Integer.parseInt(args[3]);
		}
		catch(NumberFormatException ex) {
			System.out.println("[Error] Can't parse column number !");
			usage(args);
			return;
		}
		///// End parsing
		
		centers = new Double[clusterNumber];
		for (int i = 0; i < clusterNumber; ++i)
		{
			centers[i] = 10.0 * i;
		}
		
		System.out.println("=== K-Means algorithm for hadoop ===");
		System.out.println("Arguments:");
		System.out.println("\tinputPath: \t" + inputPath);
		System.out.println("\toutputPath: \t" + initialOutput);
		System.out.println("\tnumber of clusters: \t" + clusterNumber);
		System.out.println("\tnumber of the column: \t" + columnNumber);
		System.out.println("");
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Settings of configuration
		conf.setInt("clusterNumber", clusterNumber);
		conf.setInt("columnNumber", columnNumber);

		while (!jobDone)
		{
			outputPath = initialOutput + System.nanoTime();
			
			for (int i = 0; i < clusterNumber; ++i)
			{
				conf.setDouble("center" + i, centers[i]);
			}
			
			// Declare the job
			job = Job.getInstance(conf, "K-Means iteration " + nbIteration);
			job.setNumReduceTasks(1);
			job.setJarByClass(App.class);
			
			/*****
			 * Formats
			 *****/
			job.setInputFormatClass(CSVInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			/*****
			 * Mapper
			 *****/
			job.setMapperClass(KIteratorMapper.class);
			
			/****
			 * Map output
			 ****/
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			
			/*****
			 * Combiner
			 *****/
			job.setCombinerClass(KIteratorCombiner.class);
			
			/****
			 * Reducer
			 ****/
			job.setReducerClass(KIteratorReducer.class);
			
			/*****
			 * Final output
			 *****/
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			/*****
			 * Paths
			 *****/
			System.out.println("KMeans iteration " + nbIteration + ":");
			System.out.println("Input: " + inputPath);
			System.out.println("Output: " + outputPath);
			TextInputFormat.addInputPath(job, new Path(inputPath));
			TextOutputFormat.setOutputPath(job, new Path(outputPath));
			
			/*****
			 * Launch and wait
			 ****/
			job.waitForCompletion(true);
			
			Double newCenters[] = new Double[clusterNumber];
			System.out.println("New centers:");
			for (int i = 0; i < clusterNumber; ++i)
			{
				// Convert back long to double :D
				newCenters[i] = Double.longBitsToDouble(job.getCounters().findCounter("centers", "" + i).getValue());
				System.out.println("\t" + i + ": " + newCenters[i]);
			}
			System.out.println("");
			
			// Test if centers converged
			boolean converged = true;
			for (int i = 0; i < clusterNumber; ++i)
			{
				if (converged && Math.abs(centers[i] - newCenters[i]) > deltaConverged)
				{
					System.out.println("Divergence found for cluster " + i);
					System.out.println("center: " + centers[i] + ", newCenter: " + newCenters[i]);
					System.out.println("Difference: " + Math.abs(centers[i] - newCenters[i]));
					converged = false;
				}
				centers[i] = newCenters[i];
			}
			
			nbIteration++;
			jobDone = converged;
		}
	}
}

