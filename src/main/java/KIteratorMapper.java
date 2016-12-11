package main.java;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Iterator mapper for the KMeans task
 * 
 * @version 1.0
 */
public class KIteratorMapper extends Mapper<LongWritable, ArrayList<String>, IntWritable, DoubleWritable> {
	
	private int clusterNumber = 0;
	private int columnNumber = 0;
	private Configuration conf = null;
	
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
	}
	
	/**
	 * Return the nearest center of a value
	 * 
	 * @param value
	 * 		We want to find the nearest center of this value
	 * 
	 * @return Nearest center
	 */
	private int getNearestCenter(Double value) {
		int nearestCenter = 0;
		double actual;
		double min = Double.MAX_VALUE;
		
		for (int i = 0; i < clusterNumber; ++i)
		{
			actual = conf.getDouble("center" + i, 0.0) - value;
			if (Math.abs(actual) < Math.abs(min))
			{
				nearestCenter = i;
				min = actual;
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
		Double point = 0.0;
		int nearestCenter = 0;
		
		try {
			point = Double.valueOf(value.get(columnNumber));
		}
		catch(NumberFormatException ex) {
			System.err.println("Error while parsing line " + key.get());
			return;
		}
		
		// Search nearest center !
		nearestCenter = getNearestCenter(point);
		
		context.write(new IntWritable(nearestCenter), new DoubleWritable(point));
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