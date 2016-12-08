package main.java;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<Object, Text, Object, Text> {
	@Override
	public void setup(Context context) {
		
	}
	
	@Override
	public void reduce(Object key, Iterable<Text> values, Context context) {
		
	}
	
	@Override
	public void cleanup(Context context) {
		
	}
}
