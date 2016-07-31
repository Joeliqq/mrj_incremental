package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

public class OWLHasValueReducer extends Reducer<LongWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	
	protected static Logger log = LoggerFactory.getLogger(OWLHasValueReducer.class);

	private Triple triple = new Triple();
	private TripleSource source = new TripleSource();
	
	private Map<Long,Collection<Long>> hasValueMap = new HashMap<Long, Collection<Long>>();
	private Map<Long,Collection<Long>> onPropertyMap = new HashMap<Long, Collection<Long>>();
	
	private Map<Long,Collection<Long>> hasValue2Map = new HashMap<Long, Collection<Long>>();
	private Map<Long,Collection<Long>> onProperty2Map = new HashMap<Long, Collection<Long>>();

	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

		Iterator<BytesWritable> itr = values.iterator();
		System.out.println("step 6");
		while (itr.hasNext()) {
			byte[] v = itr.next().getBytes();
			System.out.println("step6 has values reduce");
			if (v.length > 0) {
				if (v[0] == 0) { //Rule 14b
//					System.out.println("In OWLHasValueReducer for 14b: ");	// Added by Wugang
					long object = NumberUtils.decodeLong(v, 1);
					Collection<Long> props = onPropertyMap.get(object);
					if (props != null) {
						Collection<Long> hasValues = hasValueMap.get(object);
						if (hasValues != null) {
							triple.setSubject(key.get());
							Iterator<Long> itr2 = props.iterator();
							while (itr2.hasNext()) {
								long prop = itr2.next();
								triple.setPredicate(prop);
								Iterator<Long> itr3 = hasValues.iterator();
								while (itr3.hasNext()) { 
									long value = itr3.next();
									triple.setObject(value);
									
									// Added by WuGang
									triple.setType(TriplesUtils.OWL_HORST_14b);
									triple.setRsubject(object);							// v
									triple.setRpredicate(TriplesUtils.OWL_HAS_VALUE);	// owl:hasValue
									triple.setRobject(triple.getObject());				// w
									System.out.println("In OWLHasValueReducer for 14b output: "+triple);	// Added by Wugang														
									CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
//									context.write(source, triple);
								}
							}
						}
					}
				} else if (v[0] == 1) { //Rule 14a
//					System.out.println("In OWLHasValueReducer for 14a: ");	// Added by Wugang
					long predicate = NumberUtils.decodeLong(v, 1);
					long object = NumberUtils.decodeLong(v, 9);
					
					Collection<Long> types = hasValue2Map.get(object);
					Collection<Long> pred = onProperty2Map.get(predicate);
					if (types != null && pred != null) {
						types.retainAll(pred);
						Iterator<Long> itr4 = types.iterator();
						triple.setSubject(key.get());
						triple.setPredicate(TriplesUtils.RDF_TYPE);
						while (itr4.hasNext()) {
							long type = itr4.next();
							triple.setObject(type);
							
							// Added by WuGang
							triple.setType(TriplesUtils.OWL_HORST_14a);
							triple.setRsubject(triple.getObject());				// v
//							triple.setRpredicate(TriplesUtils.OWL_HAS_VALUE);	// owl:hasValue
							triple.setRpredicate(predicate);					// p	// Modified by WuGang, 2010-08-26,�����Ϣ�������»ָ���
							triple.setRobject(object);							// w
//							System.out.println("In OWLHasValueReducer for 14a output: "+triple);	// Added by Wugang
							
							CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
//							context.write(source, triple);
						}
					}
				}
			}
		}
	}

	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

		source.setDerivation(TripleSource.OWL_DERIVED);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
		triple.setObjectLiteral(false);

		//Load the schema triples
		CassandraDB db;
		try{
			db = new CassandraDB();
			Set<Integer> hasValueFilter = new HashSet<Integer>();
			hasValueFilter.add(TriplesUtils.DATA_TRIPLE_HAS_VALUE);
			hasValueMap = db.loadMapIntoMemory(hasValueFilter);
			hasValue2Map = db.loadMapIntoMemory(hasValueFilter, true);
			
			Set<Integer> onPropertyFilter = new HashSet<Integer>();
			onPropertyFilter.add(TriplesUtils.SCHEMA_TRIPLE_ON_PROPERTY);
			onPropertyMap = db.loadMapIntoMemory(onPropertyFilter);
			onProperty2Map = db.loadMapIntoMemory(onPropertyFilter, true);
			db.CassandraDBClose();
		}catch (TTransportException e) {
			e.printStackTrace();
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
	}

	@Override
	protected void cleanup(
			Reducer<LongWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
