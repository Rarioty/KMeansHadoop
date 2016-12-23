package main.java;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import main.java.format.CSVInputFormat;
import main.java.format.CSVOutputFormat;
import main.java.writables.PointWritable;

/**
 * Main class for the program
 *
 * @version 1.0
 */
public class App
{
	private static final double DELTA_CONVERGED = 0.01;
	
	/**
	 * Print the usage in the stdout
	 * 
	 * @param args
	 * 		Arguments pass to the program
	 */
	public static void usage(String[] args){
		System.out.println("Usage: App inputPath outputPath k N [c...]");
		System.out.println("\tinputPath: Filepath of the input csv file");
		System.out.println("\toutputPath: Filepath of the output csv file");
		System.out.println("\tk: Number of clusters to use");
		System.out.println("\tN: Hierarchical level wanted");
		System.out.println("\tc: Columns in the file to use (multiple values)");
	}
	
	/**
	 * Critical function to compute distance between two n-dimensionals points.
	 * The distance is squared to avoir dump sqrt slowness
	 * 
	 * @param first
	 * 		First point
	 * @param second
	 * 		Second point
	 * @param columnNumber
	 * 		Number of dimensions
	 * 
	 * @return Distance between this two points
	 */
	public static Double squaredDistance(Double[] first, Double[] second, int columnNumber)
	{
		Double distance = 0.0;
		
		for (int i = 0; i < columnNumber; ++i)
		{
			distance += Math.abs(second[i] - first[i]) * Math.abs(second[i] - first[i]);
		}
		
		return distance;
	}
	
	/**
	 * This function allow us to get the first n lines of the input file
	 * in order to get the centers. If there is less lines that required clusters,
	 * we return the right amount of new clusters which is the number of lines
	 * 
	 * @throws IOException
	 * 
	 * @param input
	 * 		Input filepath
	 * @param clusterNumber
	 * 		Required number of clusters
	 * @param columnNumber
	 * 		Required column number to parse
	 * 
	 * @return An array full of the readed centers
	 */
	private static Double[][] readCenters(String input, int clusterNumber, int columnNumber, int columns[]) throws IOException {
		int i = 0;
		Configuration conf = new Configuration();
		Double centers[][] = new Double[clusterNumber][];
		FileSystem fs = FileSystem.get(conf);
		Path filepath = new Path(conf.get("fs.defaultFS") + "/" + input);
		
		FSDataInputStream stream = fs.open(filepath);
		
		// For each cluster asked
		for (i = 0; i < clusterNumber; ++i)
		{
			centers[i] = new Double[columnNumber];
			
			// Read a line
			@SuppressWarnings("deprecation")
			String line = stream.readLine();
			// Dismiss if we already ended the file
			if (line == null)
				break;
			
			// Split the csv values
			String[] values = line.split(",");
			// Check if the columnNumber is available
			if (values.length >= columnNumber)
			{
				// For each columns !
				for (int j = 0; j < columnNumber; ++j)
				{
					// Finally try to convert to a double !
					try {
						centers[i][j] = Double.valueOf(values[columns[j]]);	
					} catch (Exception e) {
						System.out.print(e.getMessage());
						e.printStackTrace(System.out);
						break;
					}
				}
			}
			else
			{
				break;
			}
		}
		
		// In case we ask for 10 clusters but with only 4 lines for example :/
		Double returned[][] = new Double[i][];
		for (int j = 0; j < i; ++j)
		{
			returned[j] = new Double[columnNumber];
			for (int k = 0; k < columnNumber; ++k)
			{
				returned[j][k] = centers[j][k];
			}
		}
		
		return returned;
	}
	
	// Fonction recursive
	private static void kmeans(String inputPath, String initialOutput, int hierarchicalLevel, int currentHierarchicalLevel, int clusterNumber, int columnNumber, int[] columns, Vector<Integer> previousCluster) throws IOException, ClassNotFoundException, InterruptedException
	{
		int nbIteration = 0;
		boolean jobDone = false;
		Job job = null;
		Double centers[][];
		long startIteration;
		long endIteration;
		String outputPath;
		
		System.out.println("Kmeans function called !");
		System.out.println("inputPath: " + inputPath);
		System.out.println("initialOutput: " + initialOutput);
		System.out.println("current: " + currentHierarchicalLevel);
		System.out.print("Previous: ");
		for (int i = 0; i < previousCluster.size(); ++i)
		{
			System.out.print(previousCluster.get(i) + " ");
		}
		System.out.println("");
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Reads centers in the input file
		centers = readCenters(inputPath, clusterNumber, columnNumber, columns);
		// Redefine the number of clusters with the number of lines in the file
		clusterNumber = centers.length;
		
		// Show all columns
		System.out.print("\tcolumns: \t");
		for (int i = 0; i < columnNumber; ++i)
		{
			System.out.print(columns[i] + " ");
			conf.setInt("column" + i, columns[i]);
		}
		System.out.println("\n");
		
		// Show all centers
		System.out.println("" + clusterNumber + " centers read:");
		for (int i = 0; i < clusterNumber; ++i)
		{
			System.out.print("\t- center " + i + ": (");
			for (int j = 0; j < columnNumber; ++j)
			{
				System.out.print(centers[i][j] + " ");
				conf.setDouble("center" + i + "_" + j,  centers[i][j]);
			}
			System.out.println(")");
		}
		
		// Setup the configuration
		conf.setInt("clusterNumber", clusterNumber);
		conf.setInt("columnNumber", columnNumber);
		conf.set("outputName", initialOutput);
		conf.setInt("hierarchicalLevel", currentHierarchicalLevel);
		
		for (int i = 0; i < currentHierarchicalLevel; ++i)
		{
			conf.setInt("cluster" + i, previousCluster.get(i));
		}

		// Launch each iteration
		while (!jobDone)
		{
			// Generate new outputPath
			outputPath = initialOutput + "_" + currentHierarchicalLevel;
			for (int i = 0; i < previousCluster.size(); ++i)
			{
				outputPath += "_" + previousCluster.get(i);
			}
			outputPath += "_" + System.nanoTime();
			
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
			job.setMapOutputValueClass(PointWritable.class);
			
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
			job.setOutputValueClass(PointWritable.class);
			
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
			startIteration = System.nanoTime();
			job.waitForCompletion(true);
			endIteration = System.nanoTime();
			
			System.out.println("This iteration took " + (endIteration - startIteration) / 1000000 + " milliseconds");
			
			// Read all new generated centers
			Double newCenters[][] = new Double[clusterNumber][];
			System.out.println("New centers:");
			for (int i = 0; i < clusterNumber; ++i)
			{
				newCenters[i] = new Double[columnNumber];

				System.out.print("\t" + i + ": ");
				for (int j = 0; j < columnNumber; ++j)
				{
					// Convert back long to double :D
					newCenters[i][j] = Double.longBitsToDouble(job.getCounters().findCounter("centers", "" + i + "_" + j).getValue());
					System.out.print(newCenters[i][j] + " ");
				}
			}
			System.out.println("");
			
			// Test if centers converged
			boolean converged = true;
			for (int i = 0; i < clusterNumber; ++i)
			{
				if (converged && App.squaredDistance(centers[i], newCenters[i], columnNumber) > DELTA_CONVERGED)
				{
					System.out.println("Divergence found for cluster " + i);
					System.out.println("Difference: " + App.squaredDistance(centers[i], newCenters[i], columnNumber));
					converged = false;
				}
				centers[i] = newCenters[i];
				for (int j = 0; j < columnNumber; ++j)
				{
					conf.setDouble("center" + i + "_" + j, centers[i][j]);
				}
			}
			
			// Increment the iteration number
			// and break the loop if we converged
			nbIteration++;
			jobDone = converged;
		}
		// Iterating done !
		
		// Now we have to save the data !
		job = Job.getInstance(conf, "K-Means final");
		job.setNumReduceTasks(1);
		job.setJarByClass(App.class);
		
		/*****
		 * Formats
		 *****/
		job.setInputFormatClass(CSVInputFormat.class);
		job.setOutputFormatClass(CSVOutputFormat.class);
		
		/*****
		 * Mapper
		 *****/
		job.setMapperClass(KMapper.class);
		
		/****
		 * Map output
		 ****/
		job.setMapOutputKeyClass(IntWritable.class);
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
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		/*****
		 * Paths
		 *****/
		CSVInputFormat.addInputPath(job, new Path(inputPath));
		outputPath = initialOutput + "_" + currentHierarchicalLevel;
		for (int i = 0; i < previousCluster.size(); ++i)
		{
			outputPath += "_" + previousCluster.get(i);
		}
		CSVOutputFormat.setOutputPath(job, new Path(outputPath));
		
		/*****
		 * Launch and wait
		 ****/
		job.waitForCompletion(true);
		
		if (currentHierarchicalLevel < hierarchicalLevel-1)
		{
			// For each clusters, relaunch kmeans on the sub-cluster
			for (int i = 0; i < clusterNumber; ++i)
			{
				String newInputPath = initialOutput + "_" + currentHierarchicalLevel;
				for (int j = 0; j < previousCluster.size(); ++j)
				{
					newInputPath += "_" + previousCluster.get(j);
				}
				newInputPath += "/" + initialOutput.split("/")[initialOutput.split("/").length-1] + ".csv";
				Vector<Integer> newVector = (Vector<Integer>) previousCluster.clone();
				newVector.addElement(i);
				kmeans(newInputPath, initialOutput, hierarchicalLevel, currentHierarchicalLevel+1, clusterNumber, columnNumber, columns, newVector);
			}
		}
		
		// Here All sub-kmeans are done ! We so have the file for this level complete
		// but in fragments... We have to find a way to reassemble it !
	}
	
	private static void constructPaths(String[] paths, int start, int end)
	{
		int middle = start + (end-start)/2;
		for (int i = start; i < middle; ++i)
		{
			paths[i] += "_" + 0;
		}
		for (int i = middle; i < end; ++i)
		{
			paths[i] += "_" + 1;
		}
		
		if (end-start != 2)
		{
			constructPaths(paths, start, middle);
			constructPaths(paths, middle, end);
		}
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
		if (args.length < 5)
		{
			usage(args);
			return;
		}
		
		String inputPath = args[0];
		String initialOutput = args[1];
		int columnNumber = args.length - 4;
		int clusterNumber = 0;
		int hierarchicalLevel = 0;
		int currentHierarchicalLevel = 0;
		int columns[];
		
		try {
			clusterNumber = Integer.parseInt(args[2]);
		} catch(NumberFormatException ex) {
			System.out.println("[Error] Can't parse cluster number !");
			usage(args);
			return;
		}
		
		try {
			hierarchicalLevel = Integer.parseInt(args[3]);
		} catch(NumberFormatException ex) {
			System.out.println("[Error] Can't parse the hierarchical level !");
			usage(args);
			return;
		}
		
		// Try to parse all columns
		columns = new int[columnNumber];
		for (int i = 0; i < columnNumber; ++i)
		{
			try {
				columns[i] = Integer.parseInt(args[4+i]);
			}
			catch(NumberFormatException ex) {
				System.out.println("[Error] Can't parse column number !");
				usage(args);
				return;
			}
		}
		///// End parsing
		
		System.out.println("=== K-Means hierarchical algorithm for hadoop ===");
		System.out.println("Arguments:");
		System.out.println("\tinputPath: \t" + inputPath);
		System.out.println("\toutputPath: \t" + initialOutput);
		System.out.println("\tnumber of clusters: \t" + clusterNumber);
		System.out.println("\thierarchical levels: \t" + hierarchicalLevel);
		System.out.println("\tnumber of columns: \t" + columnNumber);
		
		kmeans(inputPath, initialOutput, hierarchicalLevel, currentHierarchicalLevel, clusterNumber, columnNumber, columns, new Vector<Integer>());
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// Generating all filenames
		String initialPath = initialOutput + "_" + (hierarchicalLevel-1);
		int nbPaths = (int) Math.pow(2, (hierarchicalLevel-1));
		String stringPaths[] = new String[nbPaths];
		for (int i = 0; i < nbPaths; ++i)
		{
			stringPaths[i] = initialPath;
		}
		
		constructPaths(stringPaths, 0, nbPaths);
		
		Path paths[] = new Path[nbPaths];
		
		for (int i = 0; i < nbPaths; ++i)
		{
			paths[i] = new Path(stringPaths[i] + "/" + initialOutput.split("/")[initialOutput.split("/").length-1] + ".csv");
			fs.rename(paths[i], new Path(stringPaths[i] + ".csv"));
			paths[i] = new Path(stringPaths[i] + ".csv");
		}
		
		FileSystem.create(fs, new Path(initialOutput + ".csv"), FsPermission.getDefault());
		fs.close();
		fs = FileSystem.get(conf);
		fs.concat(new Path(initialOutput + ".csv"), paths);
		
		// App done !
	}
}

