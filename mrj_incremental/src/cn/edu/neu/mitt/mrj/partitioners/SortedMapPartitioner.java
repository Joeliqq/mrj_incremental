/**
 * 
 */
package cn.edu.neu.mitt.mrj.partitioners;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author gibeon
 *
 */
public class SortedMapPartitioner extends Partitioner<SortedMapWritable, LongWritable> {

	@Override
	public int getPartition(SortedMapWritable key, LongWritable value, int numPartitions) {
		int partition = Math.abs(key.keySet().toString().hashCode() % numPartitions);
		return partition;
	}

}
