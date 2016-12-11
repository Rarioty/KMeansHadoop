package main.java;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Iterator reducer of KMeans task
 * 
 * @version 1.0
 */
public class KReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	/**
	 * Setup the reducer
	 * 
	 * @param context
	 * 		The context of the task
	 */
	@Override
	public void setup(Context context) {
		
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
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		Double sum = 0.0;
		int numElems = 0;
		Iterator<DoubleWritable> it = values.iterator();
		
		while (it.hasNext())
		{
			sum += it.next().get();
			numElems++;
		}
		
		context.write(key, new DoubleWritable(sum / numElems));
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
