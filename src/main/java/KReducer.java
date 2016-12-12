package main.java;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Final reducer of KMeans task
 * 
 * @version 1.0
 */
public class KReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
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
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		
		while (it.hasNext())
		{
			context.write(key, it.next());
		}
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
