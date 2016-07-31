package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

public class OWLEquivalenceSCSPMapper extends Mapper<Long, Row, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory.getLogger(OWLEquivalenceSCSPMapper.class);
	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	private byte[] bValues = new byte[17];
	
	private static HashMap<Long, Integer> subclassSchemaTriples = null;
	private static HashMap<Long, Integer> subpropSchemaTriples = null;
	
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
//		System.out.println("Into OWLEquivalenceSCSPMapper, processing: " + value);
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);
		
		if (value.getSubject() == value.getObject())
			return;
		
//		System.out.println("subclassSchemaTriples: " + subclassSchemaTriples);
		
		oKey.set(value.getSubject());
		NumberUtils.encodeLong(bValues, 1, value.getObject());
		
		if (value.getPredicate() == TriplesUtils.RDFS_SUBCLASS
				&& subclassSchemaTriples.containsKey(value.getObject())) {
			if (subclassSchemaTriples.get(value.getObject()) == 1) {
				CassandraDB.addedtriple2 = true;
				CassandraDB.stopReasoner();
			}
			if (CassandraDB.stopreasoner == true) {
				CassandraDB.stopreasoner = false;
				return;
			}
//			System.out.println("In OWLEquivalenceSCSPMapper, meet a subclassof triple: " + value);
			bValues[0] = 0;
			oValue.set(bValues, 0, 9);			
			context.write(oKey, oValue);			
		}

		if (value.getPredicate() == TriplesUtils.OWL_EQUIVALENT_CLASS) {
			bValues[0] = 2;
			oValue.set(bValues, 0, 9);			
			context.write(oKey, oValue);
			
			oKey.set(value.getObject());
			NumberUtils.encodeLong(bValues, 1, value.getSubject());
			oValue.set(bValues, 0, 9);			
			context.write(oKey, oValue);
		}

		if (value.getPredicate() == TriplesUtils.RDFS_SUBPROPERTY
				&& subpropSchemaTriples.containsKey(value.getObject())) {
			if (subpropSchemaTriples.get(value.getObject()) == 1) {
				CassandraDB.addedtriple2 = true;
				CassandraDB.stopReasoner();
			}
			if (CassandraDB.stopreasoner == true) {
				CassandraDB.stopreasoner = false;
				return;
			}
			bValues[0] = 1;
			oValue.set(bValues, 0, 9);			
			context.write(oKey, oValue);
		}

		if (value.getPredicate() == TriplesUtils.OWL_EQUIVALENT_PROPERTY) {
			bValues[0] = 3;
			oValue.set(bValues, 0, 9);			
			context.write(oKey, oValue);
			
			oKey.set(value.getObject());
			NumberUtils.encodeLong(bValues, 1, value.getSubject());
			oValue.set(bValues, 0, 9);			
			context.write(oKey, oValue);
		}
	}
	
	@Override
	public void setup(Context context) throws IOException {

		CassandraDB db;
		try {
			db = new CassandraDB();
			
			if (subpropSchemaTriples == null) {
				subpropSchemaTriples = new HashMap<>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBPROPERTY);
				//modified 2015/5/31
				db.loadSetIntoMemory(subpropSchemaTriples, filters, -1);
			}
			
			if (subclassSchemaTriples == null) {
				subclassSchemaTriples = new HashMap<>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBCLASS);
				//modified 2015/5/31
				db.loadSetIntoMemory(subclassSchemaTriples, filters, -1);
			}
			db.CassandraDBClose();
			//modified 2015/5/31
//		} catch (TTransportException e) {
//			e.printStackTrace();
//		} catch (InvalidRequestException e) {
//			e.printStackTrace();
//		} catch (UnavailableException e) {
//			e.printStackTrace();
//		} catch (TimedOutException e) {
//			e.printStackTrace();
//		} catch (SchemaDisagreementException e) {
//			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		


	}
	
}
