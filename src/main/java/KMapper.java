package main.java;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<Object, Text, Object, Text> {
	@Override
	public void setup(Context context) {
		
	}
	
	@Override
	public void map(Object key, Text value, Context context) {
		
	}
	
	@Override
	public void cleanup(Context context) {
		
	}
}
