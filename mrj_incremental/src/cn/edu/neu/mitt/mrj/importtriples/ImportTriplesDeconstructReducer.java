package cn.edu.neu.mitt.mrj.importtriples;

import java.io.IOException;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;

public class ImportTriplesDeconstructReducer extends Reducer<Text, LongWritable, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory.getLogger(ImportTriplesDeconstructReducer.class);
	private LongWritable oKey = new LongWritable();
	private long counter = 0;
	
	private CassandraDB db = null;

	private BytesWritable tripleValue = new BytesWritable();
	private BytesWritable dictValue = new BytesWritable();

	public void reduce(Text key, Iterable<LongWritable> values,  Context context)throws IOException, InterruptedException {
//		System.out.println("URI: " + key);
		
		if (key.toString().startsWith("@FAKE")) {
			String id = key.toString().substring(key.toString().indexOf('-') + 1);
			oKey.set(Long.valueOf(id));
		} else {
			oKey.set(counter++);
			//Save dictionary
			byte[] bytes = key.toString().getBytes();
			dictValue.setSize(bytes.length + 1);
			System.arraycopy(bytes, 0, dictValue.getBytes(), 0, bytes.length);
			dictValue.getBytes()[bytes.length] = 1;
			context.write(oKey, dictValue);
			
			// Added by WuGang 2015-01-21, for writing to COLUMNFAMILY_RESOURCE
			try {
				db.insertResources(counter-1, key.toString());
			} catch (InvalidRequestException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
		}
		
		//output node id + triple id
		for(LongWritable value : values) {
			tripleValue.setSize(9);
			tripleValue.getBytes()[8] = 0;
			NumberUtils.encodeLong(tripleValue.getBytes(), 0, value.get());
		    context.write(oKey, tripleValue);
		    
//		    System.out.println("node id: " + oKey);
//		    System.out.println("triple id: " + tripleValue);
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

		
    	String taskId = context.getConfiguration().get("mapred.task.id").substring(
    			context.getConfiguration().get("mapred.task.id").indexOf("_r_") + 3);
		taskId = taskId.substring(0, taskId.indexOf('_'));
		counter = (Long.valueOf(taskId) + 1) << 32;
		log.debug("Start counter " + (Long.valueOf(taskId) + 1));
    }

	protected void cleanup(Context context) throws IOException, InterruptedException{
		db.CassandraDBClose();
	}
	
}
