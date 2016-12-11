package main.java;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<LongWritable, ArrayList<String>, IntWritable, DoubleWritable> {
	
	private int clusterNumber = 0;
	private int columnNumber = 0;
	private Configuration conf = null;
	
	@Override
	public void setup(Context context) {
		conf = context.getConfiguration();
		clusterNumber = conf.getInt("clusterNumber", 1);
		columnNumber = conf.getInt("columnNumber", 0);
	}
	
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
		
		if (key.get() < clusterNumber)
		{
			conf.setDouble("center" + key.get(), point);
			nearestCenter = (int) key.get();
		}
		else
		{
			// Search nearest center !
			nearestCenter = getNearestCenter(point);
		}
		
		context.write(new IntWritable(nearestCenter), new DoubleWritable(point));
	}
	
	@Override
	public void cleanup(Context context) {
		
	}
}
