package cn.edu.neu.mitt.mrj.io.files.writers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FilesDictWriter extends SequenceFileOutputFormat<LongWritable, BytesWritable> {

	public static void writeCommonResources(Configuration conf, boolean cond) {
		conf.setBoolean("writeCommonResources", cond);
	}
	
	//This method will change the path adding the prefix "_dict/commons" or "_dict/others"
	public Path getDefaultWorkFile(TaskAttemptContext context,
            String extension) throws IOException{
		Path old = super.getDefaultWorkFile(context, extension);
		String name = old.getName();
		if (context.getConfiguration().getBoolean("writeCommonResources", false)) {
			name = "_dict/commons/" + name;			
		} else {
			name = "_dict/others/" + name;
		}
		return new Path(old.getParent(),name);
	}
}
