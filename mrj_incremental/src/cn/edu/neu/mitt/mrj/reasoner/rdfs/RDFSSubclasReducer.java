package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

public class RDFSSubclasReducer
		extends
		Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubclasReducer.class);

	public static Map<Long, Collection<Long>> subclassSchemaTriples = null;
	protected Set<Long> subclasURIs = new HashSet<Long>();
	protected Set<Long> existingURIs = new HashSet<Long>();
	protected HashMap<Long, Integer> memberProperties = null;
	protected Set<Long> specialSuperclasses = new HashSet<Long>();
	private TripleSource source = new TripleSource();
	private Triple oTriple = new Triple();
	private Map<String, ByteBuffer> keys = 	new LinkedHashMap<String, ByteBuffer>();
	private Map<String, ByteBuffer> allkeys = 	new LinkedHashMap<String, ByteBuffer>();
	private List<ByteBuffer> allvariables =  new ArrayList<ByteBuffer>();
	private List<ByteBuffer> allTValues =  new ArrayList<ByteBuffer>();
	private List<ByteBuffer> stepsValues =  new ArrayList<ByteBuffer>();
	private void recursiveScanSuperclasses(long value, Set<Long> set) {
		Collection<Long> subclassValues = subclassSchemaTriples.get(value);
		if (subclassValues != null) {
			Iterator<Long> itr = subclassValues.iterator();
			while (itr.hasNext()) {
				long classValue = itr.next();
				if (!set.contains(classValue)) {
					set.add(classValue);
					recursiveScanSuperclasses(classValue, set);
				}
			}
		}
	}

	@Override
	public void reduce(BytesWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		// System.out.println("����RDFSSubclasReducer��-");

		existingURIs.clear();

		Iterator<LongWritable> itr = values.iterator();
		while (itr.hasNext()) {
			long value = itr.next().get();
			existingURIs.add(value); // ���еı���
		}

		Iterator<Long> oTypes = existingURIs.iterator();
		subclasURIs.clear();
		while (oTypes.hasNext()) {
			long existingURI = oTypes.next();
			recursiveScanSuperclasses(existingURI, subclasURIs); // subclasURIs�����е�subclass
		}

		subclasURIs.removeAll(existingURIs);

		oTypes = subclasURIs.iterator();
		byte[] bKey = key.getBytes();
		long oKey = NumberUtils.decodeLong(bKey, 1);
		oTriple.setSubject(oKey);
		boolean typeTriple = bKey[0] == 0;
		if (!typeTriple) { // It's a subclass triple
			oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS); // Rule 11
			// Added by WuGang, 2010-08-26
			oTriple.setType(TriplesUtils.RDFS_11);
			oTriple.setRsubject(oTriple.getSubject());
			oTriple.setRpredicate(TriplesUtils.RDFS_SUBCLASS);
		} else { // It's a type triple
			oTriple.setPredicate(TriplesUtils.RDF_TYPE); // Rule 9
			// Added by WuGang, 2010-08-26
			oTriple.setType(TriplesUtils.RDFS_9);
			oTriple.setRsubject(oTriple.getSubject());
			oTriple.setRpredicate(TriplesUtils.RDF_TYPE);
		}
		// while (oTypes.hasNext()) {
		// long oType = oTypes.next();
		// oTriple.setObject(oType);
		// context.write(source, oTriple);
		// }
		// Modified by WuGang, 2010-08-26
		while (oTypes.hasNext()) {
			long oType = oTypes.next();
			oTriple.setObject(oType);
			for (long obj : existingURIs) {
				oTriple.setRobject(obj);
				context.getCounter("RDFS derived triples", "subproperty of member").increment(1);
				CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 		
				// context.write(source, oTriple);
			}
		}

		if (typeTriple) {
			/* Check special rules */
			if ((subclasURIs
					.contains(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY) || existingURIs
					.contains(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY))
					&& !memberProperties.containsKey(oTriple.getSubject())) { // Rule
																			// 12���μ�RDFSSpecialPropsReducer
				if (memberProperties.get(oTriple.getSubject()) == 1) {
					CassandraDB.addedtriple2 = true;
					CassandraDB.stopReasoner();
				}
				if (CassandraDB.stopreasoner == true) {
					CassandraDB.stopreasoner = false;
					return;
				}
				
				oTriple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
				oTriple.setObject(TriplesUtils.RDFS_MEMBER);
				// Added by WuGang, 2010-08-26
				oTriple.setType(TriplesUtils.RDFS_12);
				oTriple.setRsubject(oTriple.getSubject());
				oTriple.setRpredicate(TriplesUtils.RDF_TYPE);
				oTriple.setRobject(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY);

				context.getCounter("RDFS derived triples", "subproperty of member").increment(1);
				CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 		

				// context.write(source, oTriple);
				context.getCounter("RDFS derived triples",
						"subproperty of member").increment(1);
			}

			if (subclasURIs.contains(TriplesUtils.RDFS_DATATYPE)
					|| existingURIs.contains(TriplesUtils.RDFS_DATATYPE)) {
				specialSuperclasses.clear();
				recursiveScanSuperclasses(oTriple.getSubject(),
						specialSuperclasses);
				if (!specialSuperclasses.contains(TriplesUtils.RDFS_LITERAL)) { // Rule
																				// 13���μ�RDFSSpecialPropsReducer
					oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
					oTriple.setObject(TriplesUtils.RDFS_LITERAL);
					// Added by WuGang, 2010-08-26
					oTriple.setType(TriplesUtils.RDFS_13);
					oTriple.setRsubject(oTriple.getSubject());
					oTriple.setRpredicate(TriplesUtils.RDF_TYPE);
					oTriple.setRobject(TriplesUtils.RDFS_DATATYPE);

					context.getCounter("RDFS derived triples", "subproperty of member").increment(1);
					CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 		
					// context.write(source, oTriple);
					context.getCounter("RDFS derived triples",
							"subclass of Literal").increment(1);
				}
			}

			if (subclasURIs.contains(TriplesUtils.RDFS_CLASS)
					|| existingURIs.contains(TriplesUtils.RDFS_CLASS)) {
				specialSuperclasses.clear();
				recursiveScanSuperclasses(oTriple.getSubject(),
						specialSuperclasses);
				if (!specialSuperclasses.contains(TriplesUtils.RDFS_RESOURCE)) { // Rule
																					// 8���μ�RDFSSpecialPropsReducer
					oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
					oTriple.setObject(TriplesUtils.RDFS_RESOURCE);
					// Added by WuGang, 2010-08-26
					oTriple.setType(TriplesUtils.RDFS_8);
					oTriple.setRsubject(oTriple.getSubject());
					oTriple.setRpredicate(TriplesUtils.RDF_TYPE);
					oTriple.setRobject(TriplesUtils.RDFS_CLASS);

					context.getCounter("RDFS derived triples", "subproperty of member").increment(1);
					CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 		
					// context.write(source, oTriple);
					context.getCounter("RDFS derived triples",
							"subclass of resource").increment(1);
				}
			}
		}

		// Update the counters
		if (typeTriple)
			context.getCounter("RDFS derived triples",
					"subclass inheritance rule").increment(subclasURIs.size());
		else
			context.getCounter("RDFS derived triples",
					"subclass transitivity rule").increment(subclasURIs.size());
	}

	@Override
	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation(); // 2014-12-11, Very strange, this works
											// around.

		if (subclassSchemaTriples == null) {
			CassandraDB db;
			try {
				db = new CassandraDB();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBCLASS);
				subclassSchemaTriples = db.loadMapIntoMemory(filters);
				db.CassandraDBClose();
			} catch (TTransportException e) {
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

		if (memberProperties == null) {
			CassandraDB db;
			try {
				db = new CassandraDB();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_MEMBER_SUBPROPERTY);

				memberProperties = new HashMap<>();
				db.loadSetIntoMemory(memberProperties, filters, -1);

				db.CassandraDBClose();
			} catch (TTransportException e) {
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

		source.setDerivation(TripleSource.RDFS_DERIVED);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));

	}

	@Override
	protected void cleanup(
			Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		
		super.cleanup(context);
	}
}
