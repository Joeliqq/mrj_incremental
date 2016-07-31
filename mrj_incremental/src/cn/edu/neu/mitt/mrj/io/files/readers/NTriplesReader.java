package cn.edu.neu.mitt.mrj.io.files.readers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NTriplesReader extends FileInputFormat<Text, Text> {
	
	protected static Logger log = LoggerFactory.getLogger(NTriplesReader.class);
	
	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new NTriplesRecordReader();
	}
	
	private static void listFiles(Path path, PathFilter filter, List<FileStatus> result, Configuration job) throws IOException {
		FileSystem fs = path.getFileSystem(job);
		FileStatus file = fs.getFileStatus(path);
		
		if (!file.isDir()) throw new IOException("Path is not a dir");
		
		FileStatus[] children = fs.listStatus(path, filter);
		for(FileStatus child : children) {
			if (!child.isDir()) {
				result.add(child);
			} else {
				listFiles(child.getPath(), filter, result, job);
			}
		}
	}

	protected List<FileStatus> recursiveListStatus(JobContext job) throws IOException {
		List<FileStatus> listFiles = new ArrayList<FileStatus>();
		
		try {
			Path[] dirs = getInputPaths(job);
			PathFilter jobFilter = getInputPathFilter(job);
		    if (dirs.length == 0) { throw new IOException("No input paths specified in job"); }
		    
		    for (Path dir : dirs) { listFiles(dir, jobFilter, listFiles, job.getConfiguration()); }
		} catch (Exception e) { }
		
		return listFiles;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		
		long totalLength = 0;
		
		for (FileStatus file: recursiveListStatus(job)) {
			totalLength += file.getLen();
		}
		
		
		int numSplits = job.getConfiguration().getInt("maptasks", 1);
		long splitSize = (long)((float)totalLength / (float)numSplits);
		
		MultiFilesSplit split = new MultiFilesSplit();
		for (FileStatus file: recursiveListStatus(job)) {
			if (split.getLength() < splitSize) {
				split.addFile(file);
			} else {
				splits.add(split);
				split = new MultiFilesSplit();
				split.addFile(file);
			}
		}
		
		if (split.getFiles().size() > 0)
			splits.add(split);

		return splits;
	}
}
