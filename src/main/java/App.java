package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App
{
	public static void usage(String[] args){
		System.out.println("Usage: App inputPath outputPath k c");
		System.out.println("\tinputPath: Filepath of the input csv file");
		System.out.println("\toutputPath: Filepath of the output csv file");
		System.out.println("\tk: Number of clusters to use");
		System.out.println("\tc: Column in the file to use");
	}
	
	public static void main( String[] args ) throws Exception {
		///// Parse arguments
		if (args.length < 5)
		{
			usage(args);
			return;
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		int clusterNumber = 0;
		int columnNumber = 0;
		
		try {
			clusterNumber = Integer.parseInt(args[2]);
		}
		catch(NumberFormatException ex) {
			System.out.println("[Error] Can't parse cluster number !");
			usage(args);
			return;
		}
		
		try {
			columnNumber = Integer.parseInt(args[2]);
		}
		catch(NumberFormatException ex) {
			System.out.println("[Error] Can't parse cluster number !");
			usage(args);
			return;
		}
		///// End parsing
		
		System.out.println("=== K-Means algorithm for hadoop ===");
		System.out.println("Arguments:");
		System.out.println("\tinputPath: \t" + inputPath);
		System.out.println("\toutputPath: \t" + outputPath);
		System.out.println("\tnumber of clusters: \t" + clusterNumber);
		System.out.println("\tnumber of the column: \t" + columnNumber);
		System.out.println("");
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Declare the job
		Job job = Job.getInstance(conf, "K-Means");
		job.setNumReduceTasks(1);
		job.setJarByClass(App.class);
		
		/*****
		 * Mapper
		 *****/
		job.setMapperClass(KMapper.class);
		
		/****
		 * Map output
		 ****/
		job.setMapOutputKeyClass(Object.class);
		job.setMapOutputValueClass(Text.class);
		
		/*****
		 * Combiner
		 *****/
		job.setCombinerClass(KCombiner.class);
		
		/****
		 * Reducer
		 ****/
		job.setReducerClass(KReducer.class);
		
		/*****
		 * Final output
		 *****/
		job.setOutputKeyClass(Object.class);
		job.setOutputValueClass(Text.class);
		
		TextOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
}

