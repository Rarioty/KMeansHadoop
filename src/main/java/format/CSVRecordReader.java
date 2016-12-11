package main.java.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/***
 * This class is used for reading a line from the CSV file
 * 
 * @version 1.0
 */
public class CSVRecordReader extends RecordReader<LongWritable, List<String>> {
	private LineRecordReader lineReader;
	private long line;
	
	/**
	 * Constructor
	 * 
	 * @throws IOException
	 */
	public CSVRecordReader() throws IOException {
		lineReader = new LineRecordReader();
		line = 0;
	}
	
	/**
	 * Close the reader
	 * 
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	/**
	 * Return the next key for the mapper
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @return Generated key
	 */
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return new LongWritable(line++);
	}

	/**
	 * Generate the value for the mapper
	 * The value is a list of all values without commas
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @return Generated value 
	 */
	@Override
	public List<String> getCurrentValue() throws IOException, InterruptedException {
		return new ArrayList<String>(Arrays.asList(lineReader.getCurrentValue().toString().split(",")));
	}

	/**
	 * Return the progress of the read task
	 * 
	 * @throws IOException
	 * @throws InterrupedException
	 * @return A percentage of the progress
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineReader.getProgress();
	}

	/**
	 * Initialize the reader
	 * 
	 * @throws IOException
	 * @throws InterrupedException
	 * 
	 * @param input
	 * 		Input split to read into
	 * @param context
	 * 		context of the job
	 */
	@Override
	public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException {
		lineReader.initialize(input, context);
	}

	/**
	 * Return true if there is a next line
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 * @return true if there is a next line
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lineReader.nextKeyValue();
	}

}
