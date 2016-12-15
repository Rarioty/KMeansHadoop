package main.java;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import main.java.writables.PointWritable;

/**
 * Iterator mapper for the KMeans task
 * 
 * @version 1.0
 */
public class KIteratorMapper extends Mapper<LongWritable, ArrayList<String>, IntWritable, PointWritable> {
	
	private int clusterNumber = 0;
	private int columnNumber = 0;
	private Configuration conf = null;
	private Double[][] centers;
	private int[] columns;
	
	/**
	 * Setup the mapper
	 * 
	 * @param context
	 * 		Context of the task
	 */
	@Override
	public void setup(Context context) {
		conf = context.getConfiguration();
		clusterNumber = conf.getInt("clusterNumber", 1);
		columnNumber = conf.getInt("columnNumber", 0);
		
		// Generate all centers in memory
		centers = new Double[clusterNumber][];
		for (int i = 0; i < clusterNumber; ++i)
		{
			centers[i] = new Double[columnNumber];
			for (int j = 0; j < columnNumber; ++j)
			{
				centers[i][j] = conf.getDouble("center" + i + "_" + j, 0.0);
			}
		}
		
		// Get all columns numbers
		columns = new int[columnNumber];
		for (int i = 0; i < columnNumber; ++i)
		{
			columns[i] = conf.getInt("column" + i, -1);
		}
	}
	
	/**
	 * Return the nearest center of a value
	 * 
	 * @param value
	 * 		We want to find the nearest center of this value
	 * 
	 * @return Nearest center
	 */
	private int getNearestCenter(Double[] value) {
		int nearestCenter = 0;
		double actual;
		double minDistance = Double.MAX_VALUE;
		
		for (int i = 0; i < clusterNumber; ++i)
		{
			actual = App.squaredDistance(value, centers[i], columnNumber);
			if (actual < minDistance)
			{
				nearestCenter = i;
				minDistance = actual;
			}
		}
		
		return nearestCenter;
	}
	
	/**
	 * The map function of the mapper
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 * @param key
	 * 		The key -> Line of the file
	 * @param value
	 * 		The value -> List of all values without commas
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void map(LongWritable key, ArrayList<String> value, Context context) throws IOException, InterruptedException {
		// Get double value
		Double[] point = new Double[columnNumber];
		int nearestCenter = 0;
		
		try {
			for (int i = 0; i < columnNumber; ++i)
			{
				point[i] = Double.valueOf(value.get(columns[i]));
			}
		}
		catch(NumberFormatException ex) {
			System.err.println("Error while parsing line " + key.get());
			return;
		}
		
		// Search nearest center !
		nearestCenter = getNearestCenter(point);
		
		context.write(new IntWritable(nearestCenter), new PointWritable(columnNumber, point));
	}
	
	/**
	 * Cleanup the mapper
	 * 
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void cleanup(Context context) {
		
	}
}
