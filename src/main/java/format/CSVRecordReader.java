package main.java.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class CSVRecordReader extends RecordReader<LongWritable, List<String>> {
	private LineRecordReader lineReader;
	private long line;
	
	public CSVRecordReader(FileSplit input, TaskAttemptContext context) throws IOException {
		lineReader = new LineRecordReader();
		line = 0;
	}
	
	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return new LongWritable(line++);
	}

	@Override
	public List<String> getCurrentValue() throws IOException, InterruptedException {
		return new ArrayList<String>(Arrays.asList(lineReader.getCurrentValue().toString().split(",")));
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineReader.getProgress();
	}

	@Override
	public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException {
		lineReader.initialize(input, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lineReader.nextKeyValue();
	}

}
