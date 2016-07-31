/**
 * Project Name: mrj-0.1
 * File Name: ImportTriplesReconstructReducerToCassandra.java
 * @author Gang Wu
 * 2014锟斤拷10锟斤拷28锟斤拷 锟斤拷锟斤拷10:35:24
 * 
 * Description: 
 * Send reducer output to Cassandra DB by representing triples with ids
 */
package cn.edu.neu.mitt.mrj.importtriples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.cli.CliParser.rowKey_return;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;


/**
 * @author gibeo_000
 *
 */
public class ImportTriplesReconstructReducerToCassandra extends
		Reducer<LongWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	protected static Logger log = LoggerFactory.getLogger(ImportTriplesReconstructReducerToCassandra.class);

	private Triple oValue = new Triple();
	private TripleSource source = new TripleSource();
	private Map<String, ByteBuffer> keys;
	
	
	// Init keys and variables
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer<LongWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Have to manually setup the config yaml
		keys = new LinkedHashMap<String, ByteBuffer>();
	}


	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<LongWritable> itr = values.iterator();
		int counter = 0;
		while (itr.hasNext()) {
			counter++;
			
			//Get the position
			long value = itr.next().get();
			long pos = value & 0x3;
			if (pos == 0) {
				oValue.setSubject(value >> 2);
			} else if (pos == 1) {
				oValue.setPredicate(value >> 2);
			} else if (pos == 2) {
				oValue.setObject(value >> 2);
				oValue.setObjectLiteral(false);
			} else if (pos == 3) {
				oValue.setObject(value >> 2);
				oValue.setObjectLiteral(true);			
			} else {
				throw new IOException("Position not clear!!");
			}
		}
		
		if (counter != 3) {
			// Modified by WuGang 2010-12-3, 锟斤拷锟�?锟斤拷3元锟斤拷锟斤拷郑锟斤拷锟斤拷锟揭拷锟斤拷锟斤拷锟�
			log.error("Found a non-triple when reconstructing. The count num is " + counter + ", and triple is " + oValue);
//			throw new IOException("Triple is not reconstructed!");
		}

		context.getCounter("output","records").increment(1);
		//context.write(oKey, oValue);
        
    	byte one = 1;
    	byte zero = 0;

	    // Prepare composite key (sub, pre, obj)
        keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(oValue.getSubject()));
        keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(oValue.getPredicate()));
        keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(oValue.getObject()));
    	// the length of boolean type in cassandra is one byte!!!!!!!!
        keys.put(CassandraDB.COLUMN_IS_LITERAL, oValue.isObjectLiteral()?ByteBuffer.wrap(new byte[]{one}):ByteBuffer.wrap(new byte[]{zero}));
        keys.put(CassandraDB.COLUMN_TRIPLE_TYPE, ByteBufferUtil.bytes(TriplesUtils.getTripleType(source, oValue.getSubject(), oValue.getPredicate(), oValue.getObject())));
        keys.put(CassandraDB.COLUMN_RULE, ByteBufferUtil.bytes(0));	// for original triple set 0 int
        keys.put(CassandraDB.COLUMN_V1, ByteBufferUtil.bytes(0L));	// for original triple set 0 long
        keys.put(CassandraDB.COLUMN_V2, ByteBufferUtil.bytes(0L));	// for original triple set 0 long
        keys.put(CassandraDB.COLUMN_V3, ByteBufferUtil.bytes(0L));	// for original triple set 0 long
        
        // Prepare variables, here is a boolean value for CassandraDB.COLUMN_IS_LITERAL
    	List<ByteBuffer> variables =  new ArrayList<ByteBuffer>();
//    	variables.add(ByteBufferUtil.bytes(0));		// It corresponds to COLUMN_INFERRED_STEPS where steps = 0 means an original triple
//    	variables.add(ByteBufferUtil.bytes(0));		// Added by WuGang, 2015-07-15, to support transitive level
        context.write(keys, variables);
	}

	
	
}
