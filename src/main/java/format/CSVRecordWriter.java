package main.java.format;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CSVRecordWriter extends RecordWriter<IntWritable, Text> {
	
	private DataOutputStream out;
	
	public CSVRecordWriter(DataOutputStream stream) {
		out = stream;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}

	@Override
	public void write(IntWritable key, Text val) throws IOException, InterruptedException {
		out.writeBytes(val.toString() + "," + key.get() + "\n");
	}

}
