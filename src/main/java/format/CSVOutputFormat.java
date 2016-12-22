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

/**
 * This output format is used for writing CSV files
 * 
 * @version 1.0
 */
public class CSVOutputFormat extends FileOutputFormat<IntWritable, Text> {

	/**
	 * Get the record writer in order to write the output to the CSV file
	 * 
	 * @throws IOException
	 * @throws InterrupedException
	 * 
	 * @param context
	 * 		Context of the job
	 * 
	 * @return a new CSVRecordWriter
	 */
	@Override
	public RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// Get the current path
		Path output = getDefaultWorkFile(context, ".csv");
		
		FileSystem fs = output.getFileSystem(context.getConfiguration());
		FSDataOutputStream fileOut = fs.create(output, false);
		
		return new CSVRecordWriter(fileOut);
	}
	
	/**
	 * This override allow us to redefine the path to the output file. So
	 * we can output the correct file name and we separate each split into
	 * different unique folders !
	 * 
	 * @throws IOException
	 * 
	 * @param context
	 * 		Context of the job
	 * @param extension
	 * 		Extension of the file
	 * 
	 * @return The new unique output filename
	 */
	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
		FileOutputCommitter committer = (FileOutputCommitter)super.getOutputCommitter(context);
		Path path = committer.getWorkPath();
		Path folder = new Path(path.toString().split("_temporary")[0]);
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		
		fs.mkdirs(folder);
		
		String outputName = context.getConfiguration().get("outputName");
		
		return new Path(folder.toString() +  "/" + outputName.split("/")[outputName.split("/").length-1] + ".csv");
	}
}
