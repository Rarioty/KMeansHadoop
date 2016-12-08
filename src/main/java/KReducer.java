package main.java;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<Object, Text, Text, IntWritable> {
	@Override
	public void setup(Context context) {
		
	}
	
	@Override
	public void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
	}
	
	@Override
	public void cleanup(Context context) {
		
	}
}
