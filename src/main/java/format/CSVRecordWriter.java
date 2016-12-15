package main.java.format;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class allow us to write csv datas
 *
 * @version 1.0
 */
public class CSVRecordWriter extends RecordWriter<IntWritable, Text> {
	
	private DataOutputStream out;
	
	/**
	 * Constructor
	 * 
	 * @param stream
	 * 		A stream to the output file
	 */
	public CSVRecordWriter(DataOutputStream stream) {
		out = stream;
	}

	/**
	 * Close the output file
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 * @param context
	 * 		Context of the job
	 */
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}

	/**
	 * Write a csv data into the output file
	 * 
	 * @throws IOException
	 * @throws InterrupedException
	 * 
	 * @param key
	 * 		Output key of the reducer -> Center associated to this point
	 * @param val
	 * 		Output value of the reducer -> Point
	 */
	@Override
	public void write(IntWritable key, Text val) throws IOException, InterruptedException {
		out.writeBytes(val.toString() + "," + key.get() + "\n");
	}

}
