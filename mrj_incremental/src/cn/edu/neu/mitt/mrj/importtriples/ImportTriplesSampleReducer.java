package cn.edu.neu.mitt.mrj.importtriples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.thrift.TException;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class ImportTriplesSampleReducer extends Reducer<Text, NullWritable, LongWritable, BytesWritable> {

	private long threshold = 0;
	private long counter = 0;
	
	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	
	private CassandraDB db = null;

	public void reduce(Text key, Iterable<NullWritable> values,  Context context) throws IOException, InterruptedException { 
		long count = 0;
		Iterator<NullWritable> itr = values.iterator();
		while (itr.hasNext()) {
			count++;			
			itr.next();
		}

		if (count > threshold) {
			oKey.set(counter++);
			byte[] bytes = key.toString().getBytes();
			oValue.set(bytes, 0, bytes.length);
			context.write(oKey, oValue);
			context.getCounter("output", "records").increment(1);
			
			// Added by WuGang 2015-01-21, for writing to COLUMNFAMILY_RESOURCE
			try {
				db.insertResources(counter, key.toString());
			} catch (InvalidRequestException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}

    protected void setup(Context context) throws IOException, InterruptedException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.
		try {
			db = new CassandraDB();
			db.init();
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}

		threshold = context.getConfiguration().getInt("reasoner.threshold", 0);
		//Set counter
		String taskId = context.getConfiguration().get("mapred.task.id").substring(
				context.getConfiguration().get("mapred.task.id").indexOf("_r_") + 3);
		taskId = taskId.replaceAll("_", "");
		counter = (Long.valueOf(taskId)) << 13;
		if (counter == 0) { counter +=100; }
    }
    protected void cleanup(Context context) throws IOException, InterruptedException{
    	db.CassandraDBClose();
    }
}
