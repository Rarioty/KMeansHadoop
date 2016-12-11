package main.java;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	@Override
	public void setup(Context context) {
		
	}
	
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
	
	@Override
	public void cleanup(Context context) {
		
	}
}
