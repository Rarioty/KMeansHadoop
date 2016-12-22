package main.java;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import main.java.writables.PointWritable;

/**
 * Iterator combiner of KMeans task
 *
 * @version 1.0
 */
public class KIteratorCombiner extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {
	private Configuration conf;
	private int columnNumber = 0;
	
	/**
	 * Setup the combiner
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
	 * Reduce function of the combiner
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 * @param key
	 * 		The key -> Id of the nearest center
	 * @param value
	 * 		The values -> A list of all the points that are attached to this cluster
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
		Double[] sum = new Double[columnNumber];
		int numElems = 0;
		Iterator<PointWritable> it = values.iterator();
		
		for (int i = 0; i < columnNumber; ++i)
		{
			sum[i] = 0.0;
		}
		
		while (it.hasNext())
		{
			PointWritable point = it.next();
			for (int i = 0; i < columnNumber; ++i)
			{
				sum[i] += point.dimensions[i];
			}
			numElems += point.weight;
		}
		
		context.write(key, new PointWritable(columnNumber, sum , numElems));
	}
	
	/**
	 * Cleanup the combiner
	 * 
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void cleanup(Context context) {
		
	}
}
