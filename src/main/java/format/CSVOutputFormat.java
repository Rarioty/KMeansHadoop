package main.java.format;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CSVOutputFormat extends FileOutputFormat<IntWritable, Text> {
	@Override
	public RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// Get the current path
		Path output = getDefaultWorkFile(context, ".csv");
		
		FileSystem fs = output.getFileSystem(context.getConfiguration());
		FSDataOutputStream fileOut = fs.create(output, false);
		
		return new CSVRecordWriter(fileOut);
	}
	
	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
		FileOutputCommitter committer = (FileOutputCommitter)super.getOutputCommitter(context);
		Path path = committer.getWorkPath();
		Path folder = new Path(path.toString().split("_temporary")[0] + getUniqueFile(context, "folder", ""));
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		
		fs.mkdirs(folder);
		
		return new Path(folder.toString() +  "/" + context.getConfiguration().get("outputName").split("/")[1] + ".csv");
	}
}
