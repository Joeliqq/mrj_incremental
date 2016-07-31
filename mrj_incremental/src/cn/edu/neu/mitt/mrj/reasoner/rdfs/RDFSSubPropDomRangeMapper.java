package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

//public class RDFSSubPropDomRangeMapper extends Mapper<TripleSource, Triple, LongWritable, LongWritable> {
public class RDFSSubPropDomRangeMapper extends Mapper<Long, Row, BytesWritable, LongWritable> {

	protected static Logger log = LoggerFactory.getLogger(RDFSSubPropDomRangeMapper.class);
	protected HashMap<Long, Integer> domainSchemaTriples = null;
	protected HashMap<Long, Integer> rangeSchemaTriples = null;

	byte[] bKey = new byte[16];	// Added by WuGang, 2010-08-26

	protected LongWritable oValue = new LongWritable(0);
//	protected LongWritable oKey = new LongWritable(0);
	protected BytesWritable oKey = new BytesWritable();	// Modified by WuGang, 2010-08-26

	private int previousExecutionStep = -1;
	private boolean hasSchemaChanged = false;
	
	public void map(Long key, Row row,  Context context) throws IOException, InterruptedException {
		// Some debug codes
//		try {
//			System.out.println("-------------------->" + "key: " + key + "; value: " + row.toString());
//		    System.out.println("ConfigHelper.getInputSplitSize - out: " + ConfigHelper.getInputSplitSize(context.getConfiguration()));
//		    System.out.println("CqlConfigHelper.getInputPageRowSize - out: " + CqlConfigHelper.getInputPageRowSize(context.getConfiguration()));
//
//			System.out.println("Input format class: " + context.getInputFormatClass().getName());
//			for (String location: context.getInputSplit().getLocations())
//				System.out.println("getInputSplit Locations: " + location);
//			System.out.println("getInputSplit Length: " + context.getInputSplit().getLength());
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}

		//update 是否需要增量更新
		if (CassandraDB.updatelabel == 1) {
			CassandraDB.addedtriple1 = false;
			int label = row.getInt(CassandraDB.COLUMN_UPDATELABEL);
			if (label == 1) {
				CassandraDB.addedtriple1 = true;
			}			
		}
		
		int step = row.getInt(CassandraDB.COLUMN_INFERRED_STEPS);
		if (!hasSchemaChanged && step <= previousExecutionStep)
			return;
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

		//Check if the predicate has a domain
		if (domainSchemaTriples.containsKey(value.getPredicate())) {
			
			if (domainSchemaTriples.get(value.getPredicate()) == 1) {
				CassandraDB.addedtriple2 = true;
				CassandraDB.stopReasoner();
			}
			if (CassandraDB.stopreasoner == true) {
				CassandraDB.stopreasoner = false;
				return;
			}
			
			NumberUtils.encodeLong(bKey,0,value.getSubject());	// Added by WuGang, 2010-08-26
			NumberUtils.encodeLong(bKey,8,value.getObject());	// Added by WuGang, 2010-08-26
//			oKey.set(value.getSubject());
			oKey.set(bKey, 0, 16);	// Modified by WuGang, 2010-08-26
			oValue.set(value.getPredicate() << 1);	// ����ͨ��oValue�����һλ��0��ȷ������ǰ�������domain
			context.write(oKey, oValue);	// ��<<s,o>, p>����ȥ, for rule 2
		}

		//Check if the predicate has a range
		if (rangeSchemaTriples.containsKey(value.getPredicate())
				&& !value.isObjectLiteral()) {
			
			if (rangeSchemaTriples.get(value.getPredicate()) == 1) {
				CassandraDB.addedtriple2 = true;
				CassandraDB.stopReasoner();
			}
			if (CassandraDB.stopreasoner == true) {
				CassandraDB.stopreasoner = false;
				return;
			}
			
			NumberUtils.encodeLong(bKey,0,value.getObject());	// Added by WuGang, 2010-08-26
			NumberUtils.encodeLong(bKey,8,value.getSubject());	// Added by WuGang, 2010-08-26
//			oKey.set(value.getObject());
			oKey.set(bKey, 0, 16);	// Modified by WuGang, 2010-08-26
			oValue.set((value.getPredicate() << 1) | 1);	// ����ͨ��oValue�����һλ��1��ȷ������ǰ�������range
			context.write(oKey, oValue);	// ��<<o, s>, p>����ȥ, for rule 3
		}
		
	}
	
	@Override
	protected void setup(Context context) throws IOException {		
		hasSchemaChanged = false;
		previousExecutionStep = context.getConfiguration().getInt("lastExecution.step", -1);
		
		try{		
			CassandraDB db = new CassandraDB();

			if (domainSchemaTriples == null) {
				domainSchemaTriples = new HashMap<>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_DOMAIN_PROPERTY);
				hasSchemaChanged = db.loadSetIntoMemory(domainSchemaTriples, filters, previousExecutionStep);
				// db not close
			}
			
			if (rangeSchemaTriples == null) {
				rangeSchemaTriples = new HashMap<>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_RANGE_PROPERTY);

				hasSchemaChanged |= db.loadSetIntoMemory(rangeSchemaTriples, filters, previousExecutionStep);
				db.CassandraDBClose();
			}
		}catch(TTransportException tte){
			tte.printStackTrace();
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
		
		// Some debug codes
		System.out.println("In mapper setup, peviousExecutionStep= " + previousExecutionStep + " and hasSchemaChanged status: " + hasSchemaChanged);
		System.out.println("Input split: " + context.getInputSplit());
		try {
			System.out.println("Input split length: " + context.getInputSplit().getLength());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
	
	
}
