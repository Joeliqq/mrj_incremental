package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
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

public class OWLNotRecursiveReducer extends Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	
	private static Logger log = LoggerFactory.getLogger(OWLNotRecursiveReducer.class);

	private Triple triple = new Triple();
	private TripleSource source = new TripleSource();
	private TripleSource transitiveSource = new TripleSource();
	private Set<Long> set = new HashSet<Long>();
	
	protected Map<Long, Collection<Long>> schemaInverseOfProperties = null;

	protected void reduce(BytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		byte[] bytes = key.getBytes();
		long rsubject=0, rpredicate=0, robject=0;
		long key1=0, key2=0, value1 = 0;
				
		switch(bytes[0]) {
//		case 0: 
//		case 1: //Functional and inverse functional property
		case 0:	// Modified by WuGang, Functional
		case 1: // Modified by WuGang, Inverse Functional
//			System.out.println("Processing Functional & Inverse Functional Property.");
			key1 = NumberUtils.decodeLong(bytes, 1);	// ����Functional������subject������Inverse Functional������object
			key2 = NumberUtils.decodeLong(bytes, 9);	// predicate
			
			long minimum = Long.MAX_VALUE;
			set.clear();
			Iterator<LongWritable> itr = values.iterator();

			while (itr.hasNext()) {
				long value = itr.next().get();
				value1 = value;	// Added by Wugang���������ֵ������Functional������ԭʼ��Ԫ���object������Inverse Functional������ԭʼ��Ԫ���subject
				if (value < minimum) {
					if (minimum != Long.MAX_VALUE)
						set.add(minimum);
					minimum = value;
				} else {
					set.add(value);
				}
			}
			
			// Added by Wugang
			long type = TriplesUtils.OWL_HORST_NA;
			if (bytes[0] == 0){	//Functional
				type = TriplesUtils.OWL_HORST_1;
				rsubject = key1;
				rpredicate = key2;
				robject = value1;
			}
			else if (bytes[0] == 1){ //Inverse Functional
				type = TriplesUtils.OWL_HORST_2;
				robject = key1;
				rpredicate = key2;
				rsubject = value1;
			}
			triple.setType(type);
			triple.setRsubject(rsubject);
			triple.setRpredicate(rpredicate);
			triple.setRobject(robject);

			
			triple.setObjectLiteral(false);
			triple.setSubject(minimum);
			triple.setPredicate(TriplesUtils.OWL_SAME_AS);
			Iterator<Long> itr2 = set.iterator();
			long outputSize = 0;
			while (itr2.hasNext()) {
				long object = itr2.next();
				triple.setObject(object);
//				System.out.println("Find a derive in functional and inverse functional property!" + triple);
//				context.write(source, triple);
				CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
				outputSize++;
			}
			context.getCounter("OWL derived triples", "functional and inverse functional property").increment(outputSize);


			break;
		case 2: //Symmetric property
//			System.out.println("Processing SymmetricProperty.");
			long subject = NumberUtils.decodeLong(bytes, 1);
			long object = NumberUtils.decodeLong(bytes, 9);
			triple.setSubject(object);
			triple.setObject(subject);
			triple.setObjectLiteral(false);
			
			// Added by WuGang
			triple.setRsubject(subject);
			triple.setRobject(object);
			triple.setType(TriplesUtils.OWL_HORST_3);

			itr = values.iterator();
			while (itr.hasNext()) {
				triple.setPredicate(itr.next().get());
				triple.setRpredicate(triple.getPredicate());	// Added by WuGang
//				context.write(source, triple);
				CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
				context.getCounter("OWL derived triples", "simmetric property").increment(1);
			}
						
			break;
		case 3: //Inverse of property
//			System.out.println("Processing inverseOf.");
			subject = NumberUtils.decodeLong(bytes, 1);
			object = NumberUtils.decodeLong(bytes, 9);
			triple.setObjectLiteral(false);
			set.clear();
			itr = values.iterator();
			while (itr.hasNext()) {
				triple.setObject(subject);
				triple.setSubject(object);
				long predicate = itr.next().get();
				
				// Added by WuGang
				triple.setType(TriplesUtils.OWL_HORST_8);
				triple.setRsubject(subject);
				triple.setRobject(object);
				triple.setRpredicate(predicate);

				/* I only output the last key of the inverse */
				Collection<Long> inverse = schemaInverseOfProperties.get(predicate);
				if (inverse != null) {
					Iterator<Long> itrInverse = inverse.iterator();
					// Added by WuGang, 2015-01-27
					long derivedPredicate = itrInverse.next();
					triple.setPredicate(derivedPredicate);		// Only one of the inverse, the others will be completed in outputInverseOf()
					//triple.setPredicate(itrInverse.next());	// Commented by WuGang 2015-01-27 
//					context.write(source, triple);
					CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
					context.getCounter("OWL derived triples", "inverse of").increment(1);
					
					// Moved to here by WuGang, 2015-01-27
					set.add(derivedPredicate);	// Modified by WuGang 2015-01-27
					//set.add(predicate);
					outputInverseOf(subject, object, predicate, set, context);	// Here will complete all the other inverse
				} else {
					log.error("Something is wrong here. This should not happen...");
				}
				
//				set.add(predicate);
//				outputInverseOf(subject, object, predicate, set, context);	// Here will complete all the other inverse
			}
			break;
		case 4:
		case 5:
			// �ⲿ���Ƿ�����inferTransitivityStatements�д�����أ��˴���û����
			//Transitive property. I copy to a temporary directory setting a special triple source
			subject = NumberUtils.decodeLong(bytes, 1);
			object = NumberUtils.decodeLong(bytes, 9);
			triple.setSubject(subject);
			triple.setObject(object);
			if (bytes[0] == 4)
				triple.setObjectLiteral(false);
			else
				triple.setObjectLiteral(true);
			
			itr = values.iterator();
			while (itr.hasNext()) {
				long predicate = itr.next().get();
				if (predicate < 0)
					transitiveSource.setDerivation(TripleSource.TRANSITIVE_DISABLED);
				else
					transitiveSource.setDerivation(TripleSource.TRANSITIVE_ENABLED);
				triple.setPredicate(Math.abs(predicate));
//				context.write(transitiveSource, triple);
				CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
				context.getCounter("OWL derived triples", "transitive property input").increment(1);
			}
		default:
			break;
		}
		
	}
	
	private void outputInverseOf(long subject, long object, long predicate, Set<Long> alreadyDerived, 
			Context context) throws IOException, InterruptedException {
		Collection<Long> col = schemaInverseOfProperties.get(predicate);
		if (col != null) {			
			Iterator<Long> itr = col.iterator();
			while (itr.hasNext()) {
				long inverseOf = itr.next();
				if (!alreadyDerived.contains(inverseOf)) {
					alreadyDerived.add(inverseOf);
					triple.setSubject(object);
					triple.setObject(subject);				
					triple.setPredicate(inverseOf);
//					context.write(source, triple);
					CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
					context.getCounter("OWL derived triples", "inverse of").increment(1);
					outputInverseOf(object, subject, inverseOf, alreadyDerived, context);
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

		source.setDerivation(TripleSource.OWL_DERIVED);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
		transitiveSource.setStep(context.getConfiguration().getInt("reasoner.step", 0));

		if (schemaInverseOfProperties == null) {
			CassandraDB db;
			try{
				db = new CassandraDB();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_INVERSE_OF);
				schemaInverseOfProperties = db.loadMapIntoMemory(filters);
				// Added by WuGang 2015-01-27,
				Map<Long, Collection<Long>> schemaInverseOfProperties_reverse = db.loadMapIntoMemory(filters, true);
				schemaInverseOfProperties.putAll(schemaInverseOfProperties_reverse);
				
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
	}

	@Override
	protected void cleanup(
			Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
