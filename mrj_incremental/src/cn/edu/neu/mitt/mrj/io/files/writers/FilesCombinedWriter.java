package cn.edu.neu.mitt.mrj.io.files.writers;


import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FilesCombinedWriter extends OutputFormat<LongWritable, BytesWritable> {
	
	SequenceFileOutputFormat<LongWritable, BytesWritable> statsFormat = 
		new SequenceFileOutputFormat<LongWritable, BytesWritable>();
	FilesDictWriter dictFormat = new FilesDictWriter();

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		dictFormat.checkOutputSpecs(context);
		statsFormat.checkOutputSpecs(context);
	}

	@Override
	public RecordWriter<LongWritable, BytesWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FilesCombinedRecordWriter(dictFormat.getRecordWriter(context),
				statsFormat.getRecordWriter(context));
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return statsFormat.getOutputCommitter(context);
	}
	
}
