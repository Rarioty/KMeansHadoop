package main.java;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import main.java.writables.PointWritable;

/**
 * Iterator reducer of KMeans task
 * 
 * @version 1.0
 */
public class KIteratorReducer extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {
	private Configuration conf;
	private int columnNumber = 0;
	
	/**
	 * Setup the reducer
	 * 
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void setup(Context context) {
		conf = context.getConfiguration();
		columnNumber = conf.getInt("columnNumber", 0);
	}
	
	/**
	 * The reduce function of the reducer
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 * @param key
	 * 		The key -> Nearest center
	 * @param values
	 * 		The values -> A list of all points that are attached to this cluster
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
		Double[] averages = new Double[columnNumber];
		int numElems = 0;
		Iterator<PointWritable> it = values.iterator();
		
		// Initialization
		for (int i = 0; i < columnNumber; ++i)
		{
			averages[i] = 0.0;
		}
		
		// Do this one to avoid the divide by zero in the giant formula
		if (it.hasNext())
		{
			PointWritable point = it.next();
			for (int i = 0; i < columnNumber; ++i)
			{
				// This formula is here because of infinity when we sum up all the points...
				// This line allow us to avoid the sum of all points but recursively update the average !
				averages[i] = point.dimensions[i] / (numElems + point.weight);
			}
			numElems += point.weight;
		}
		
		while (it.hasNext())
		{
			PointWritable point = it.next();
			for (int i = 0; i < columnNumber; ++i)
			{
				// This formula is here because of infinity when we sum up all the points...
				// This line allow us to avoid the sum of all points but recursively update the average !
				averages[i] = 1 / (1 + (point.weight / numElems)) * averages[i] + (point.dimensions[i] / (numElems + point.weight));
			}
			numElems += point.weight;
		}
		
		for (int i = 0; i < columnNumber; ++i)
		{
			// Encode double value into long
			// We can because both of them are 64 bits long ! :D
			context.getCounter("centers", "" + key.get() + "_" + i).setValue(Double.doubleToLongBits(averages[i]));
		}
		
		context.write(key, new PointWritable(columnNumber, averages, 1));
	}
	
	/**
	 * Cleanup the reducer
	 * 
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void cleanup(Context context) {
		
	}
}
