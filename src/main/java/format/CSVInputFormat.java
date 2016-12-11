package main.java.format;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * This input format is used for reading CSV files
 * 
 * @version 1.0
 */
public class CSVInputFormat extends FileInputFormat<LongWritable, List<String>> {

	/**
	 * Create a record reader in order to read the input CSV file
	 * 
	 * @throws IOException
	 * @throws InterrupedException
	 * 
	 * @param input
	 * 		Input to read into
	 * @param context
	 * 		Context of the job
	 * 
	 * @return a new CSVRecordReader
	 */
	@Override
	public RecordReader<LongWritable, List<String>> createRecordReader(InputSplit input, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new CSVRecordReader();
	}
}
