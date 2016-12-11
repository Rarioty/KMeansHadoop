package main.java.format;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CSVInputFormat extends FileInputFormat<LongWritable, List<String>> {

	@Override
	public RecordReader<LongWritable, List<String>> createRecordReader(InputSplit input, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new CSVRecordReader((FileSplit)input, context);
	}
}
