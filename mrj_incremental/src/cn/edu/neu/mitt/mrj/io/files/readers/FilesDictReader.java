package cn.edu.neu.mitt.mrj.io.files.readers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class FilesDictReader extends SequenceFileInputFormat<LongWritable, BytesWritable> {

	public static Map<String, Long> readCommonResources(Configuration context, Path dir) {
		HashMap<String, Long> map = new HashMap<String, Long>();
		
		try {
			FileSystem fs = dir.getFileSystem(context);
			FileStatus files[] =  fs.listStatus(dir, new OutputLogFilter());
			if (files != null) {
				LongWritable key = new LongWritable();
				BytesWritable value = new BytesWritable();				
				for(FileStatus file : files) {
					SequenceFile.Reader input = new SequenceFile.Reader(fs, file.getPath(), context);
					boolean nextValue = false;
					do {
						nextValue = input.next(key, value);
						if (nextValue) map.put(new String(value.getBytes(), 0, value.getLength()), key.get());
					} while (nextValue);
				}
			}
		} catch (Exception e) { 
			e.printStackTrace();
		}
		
		return map;
	}
	
	public static Map<Long, String> readInvertedCommonResources(Configuration context, Path dir) {
		HashMap<Long, String> map = new HashMap<Long, String>();
		
		try {
			FileSystem fs = dir.getFileSystem(context);
			FileStatus files[] =  fs.listStatus(dir, new OutputLogFilter());
			if (files != null) {
				LongWritable key = new LongWritable();
				BytesWritable value = new BytesWritable();				
				for(FileStatus file : files) {
					SequenceFile.Reader input = new SequenceFile.Reader(fs, file.getPath(), context);
					boolean nextValue = false;
					do {
						nextValue = input.next(key, value);
						if (nextValue) map.put(key.get(), new String(value.getBytes(), 0, value.getLength()));
					} while (nextValue);
				}
			}
		} catch (Exception e) { 
			e.printStackTrace();
		}
		
		return map;
	}
	
	public static Set<Long> readSetCommonResources(Configuration context, Path dir) {
		Set<Long> map = new HashSet<Long>();
		
		try {
			FileSystem fs = dir.getFileSystem(context);
			FileStatus files[] =  fs.listStatus(dir, new OutputLogFilter());
			if (files != null) {
				LongWritable key = new LongWritable();
				BytesWritable value = new BytesWritable();				
				for(FileStatus file : files) {
					SequenceFile.Reader input = new SequenceFile.Reader(fs, file.getPath(), context);
					boolean nextValue = false;
					do {
						nextValue = input.next(key, value);
						if (nextValue) map.add(key.get());
					} while (nextValue);
				}
			}
		} catch (Exception e) { 
			e.printStackTrace();
		}
		
		return map;
	}
}
