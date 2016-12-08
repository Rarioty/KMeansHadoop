package main.java;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void setup(Context context) {
		
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) {
		
	}
	
	@Override
	public void cleanup(Context context) {
		
	}
}
