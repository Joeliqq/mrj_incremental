package cn.edu.neu.mitt.mrj.io.files.readers;



import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import cn.edu.neu.mitt.mrj.utils.FileUtils;

abstract public class MultiFilesReader<K, V> extends FileInputFormat<K, V> {

	// Modified by WuGang 2010-12-31
//	protected static void listFiles(Path path, PathFilter filter, List<FileStatus> result, Configuration job) throws IOException {
	public static void listFiles(Path path, PathFilter filter, List<FileStatus> result, Configuration job) throws IOException {
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
	
	protected static List<FileStatus> recursiveListStatus(JobContext job) throws IOException {
		return recursiveListStatus(job, null);
	}

	
	public static List<FileStatus> recursiveListStatus(JobContext job, String filter) throws IOException {
		List<FileStatus> listFiles = new ArrayList<FileStatus>();
		
		try {
			Path[] dirs = getInputPaths(job);
			// Added by WuGang
//			for (Path p: dirs)
//				System.out.println("In recursiveListStatus: " + p + ", with filter: " + filter);
			if (filter == null) {
				filter = job.getConfiguration().get("input.filter");
				if (filter == null) {
					filter = "FILTER_ONLY_HIDDEN";
				}
			}
			PathFilter jobFilter = null;
			if (filter != null) {
				try {
				Field field = FileUtils.class.getField(filter);
				jobFilter = (PathFilter)field.get(FileUtils.class);
				} catch (Exception e) {  }
			}
		    if (dirs.length == 0) { throw new IOException("No input paths specified in job"); }
		    
		    for (Path dir : dirs) { listFiles(dir, jobFilter, listFiles, job.getConfiguration()); }
		    
		} catch (Exception e) { }
		
		return listFiles;
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		long totalLength = 0;
		List<FileStatus> files = recursiveListStatus(job);
		for (FileStatus file: files) {
			totalLength += file.getLen();
		}
		
		int numSplits = job.getConfiguration().getInt("maptasks", 1);
		long splitSize = (long)((float)totalLength / (float)numSplits);
		
		MultiFilesSplit split = new MultiFilesSplit();
		for (FileStatus file: files) {
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
