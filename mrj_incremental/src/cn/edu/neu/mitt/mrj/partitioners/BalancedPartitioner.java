package cn.edu.neu.mitt.mrj.partitioners;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BalancedPartitioner extends HashPartitioner<BytesWritable, NullWritable> implements Configurable {
	
	protected static Logger log = LoggerFactory.getLogger(BalancedPartitioner.class);
	Configuration c;
	static byte[][] partitions = null;
	
	@Override
	public int getPartition(BytesWritable key, NullWritable value, int numPartitions) {
		int beginRange = 0;
		int endRange = partitions.length;
		int index = 0;
		while (beginRange < endRange) {
			index = (endRange + beginRange) / 2;
			int cmp = BytesWritable.Comparator.compareBytes(partitions[index], 0, partitions[index].length,
												key.getBytes(), 0, key.getLength());
			if (cmp > 0) {
				endRange = index;				
			} else {
				beginRange = index + 1;
			}
		}
		return (beginRange > (numPartitions - 1)) ? numPartitions - 1 : beginRange;
	}

	@Override
	public Configuration getConf() {
		return c;
	}

	@Override
	public void setConf(Configuration c) {
		this.c = c;
		if (partitions == null) {
			try {
				ArrayList<byte[]> listPartitions = new ArrayList<byte[]>(c.getInt("mapred.reduce.tasks", 0));
				Path location = new Path(c.get("partitionsLocation"));
				FileSystem fs = location.getFileSystem(c);
				FileStatus[] files = fs.listStatus(location);
				BytesWritable key = new BytesWritable();
				NullWritable value = NullWritable.get();
				for(FileStatus file : files) {
					//Add it to the list
					SequenceFile.Reader input = null;
					try {
						input = new SequenceFile.Reader(fs, file.getPath(), c);
						boolean nextValue = false;
						do {
							nextValue = input.next(key, value);
							if (nextValue) {
								byte[] pb = Arrays.copyOf(key.getBytes(), key.getLength());
								listPartitions.add(pb);
							}
						} while (nextValue); 
						
					} finally {	
						if (input != null) {		
							input.close();
						}
					}
				}
				
				partitions = new byte[listPartitions.size()][];
				listPartitions.toArray(partitions);
			} catch (Exception e) {
				log.error("Could not load the partition table", e);
			}
		} else {
			log.debug("partition table already loaded!");
		}
	}
}