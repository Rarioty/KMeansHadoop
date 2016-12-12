package main.java;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Final combiner of KMeans task
 *
 * @version 1.0
 */
public class KCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
	/**
	 * Setup the combiner
	 * 
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void setup(Context context) {
		
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
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		
		while (it.hasNext())
		{
			context.write(key, it.next());
		}
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
