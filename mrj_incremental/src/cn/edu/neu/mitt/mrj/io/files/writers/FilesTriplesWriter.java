package cn.edu.neu.mitt.mrj.io.files.writers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

public class FilesTriplesWriter extends SequenceFileOutputFormat<TripleSource, Triple> {
	public RecordWriter<TripleSource, Triple> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new TriplesRecordWriter(context);
	}
	
	private class TriplesRecordWriter extends RecordWriter<TripleSource, Triple> {

		TaskAttemptContext context = null;
		Map<Integer,SequenceFile.Writer> files = new HashMap<Integer, SequenceFile.Writer>();
		
		public TriplesRecordWriter(TaskAttemptContext context) {
			this.context = context;
		}
		
		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			Iterator<SequenceFile.Writer> itr = files.values().iterator();
			while (itr.hasNext()) {
				itr.next().close();
			}
		}

		@Override
		public void write(TripleSource key, Triple value) throws IOException,
				InterruptedException {
			int type = TriplesUtils.getTripleType(key, value.getSubject(), value.getPredicate(), value.getObject());
			SequenceFile.Writer writer = files.get(type);
			if (writer == null) { //Create new file
				String name = TriplesUtils.getFileExtensionByTripleType(key, value, getUniqueFile(context, "triples", ""));
//				System.out.println("I'm creating a new triple file: " + name);
				FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
				Path path = new Path(committer.getWorkPath(), name);
				writer = SequenceFile.createWriter(FileSystem.get(context.getConfiguration()), context.getConfiguration(), path, 
						TripleSource.class, Triple.class, 
						CompressionType.BLOCK, new DefaultCodec(), context);
				files.put(type, writer);
			}

			writer.append(key, value);			
		}
		
	}
}
