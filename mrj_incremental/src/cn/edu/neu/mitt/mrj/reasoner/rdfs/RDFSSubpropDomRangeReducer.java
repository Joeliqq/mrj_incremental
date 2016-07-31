package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.conf.Configuration;
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

//public class RDFSSubpropDomRangeReducer extends Reducer<LongWritable, LongWritable, TripleSource, Triple> {
public class RDFSSubpropDomRangeReducer extends Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	
	protected static Logger log = LoggerFactory.getLogger(RDFSSubpropDomRangeReducer.class);
	
	protected static Map<Long, Collection<Long>> domainSchemaTriples = null;
	protected static Map<Long, Collection<Long>> rangeSchemaTriples = null;
	protected Set<Long> propURIs = new HashSet<Long>();
//	protected Set<Long> derivedProps = new HashSet<Long>();
	protected Set<Entry<Long, Long>> derivedProps = new HashSet<Entry<Long, Long>>();	// Modified by WuGang, 2010-08-26
	private TripleSource source = new TripleSource();
	private Triple oTriple = new Triple();

	private ByteBuffer outputKey;
	private Map<String, ByteBuffer> keys = 	new LinkedHashMap<String, ByteBuffer>();
	private Map<String, ByteBuffer> allkeys = 	new LinkedHashMap<String, ByteBuffer>();
	private List<ByteBuffer> allvariables =  new ArrayList<ByteBuffer>();
	private List<ByteBuffer> allTValues =  new ArrayList<ByteBuffer>();
	private List<ByteBuffer> stepsValues =  new ArrayList<ByteBuffer>();
	@Override
	public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
		byte[] bKey = key.getBytes(); // Added by Wugang, 2010-08-26
		// long uri = key.get(); //��domain���ԣ���s����range���ԣ���o
		long uri = NumberUtils.decodeLong(bKey, 0); // ��domain������s����range������o
		long uri_opposite = NumberUtils.decodeLong(bKey, 8); // ��domain������o����range������s

		Configuration conf = context.getConfiguration();
		derivedProps.clear(); // ���x

		Logger logger = LoggerFactory.getLogger(CassandraDB.class);
		long time = System.currentTimeMillis();

		// Get the predicates with a range or domain associated to this URIs
		propURIs.clear();
		Iterator<LongWritable> itr = values.iterator();
		while (itr.hasNext())
			propURIs.add(itr.next().get()); // ���p

//		 logger.info("while1 " + (System.currentTimeMillis() - time));
//		System.out.println("while1 " + (System.currentTimeMillis() - time));

		Iterator<Long> itrProp = propURIs.iterator();
		while (itrProp.hasNext()) {
			Collection<Long> objects = null;
			long propURI = itrProp.next();
			if ((propURI & 0x1) == 1) {
				objects = rangeSchemaTriples.get(propURI >> 1);
				context.getCounter("derivation", "range matches").increment(1);
			} else {
				objects = domainSchemaTriples.get(propURI >> 1);
				context.getCounter("derivation", "domain matches").increment(1);
			}

			if (objects != null) {
				Iterator<Long> itr3 = objects.iterator();
				while (itr3.hasNext())
					// derivedProps.add(itr3.next());
					derivedProps.add(new AbstractMap.SimpleEntry<Long, Long>(
							itr3.next(), propURI)); // Modified by WuGang,
													// 2010-08-26
			}
		}

//		 logger.info("while2 " + (System.currentTimeMillis() - time));
//		time = System.currentTimeMillis();
//		System.out.println("while2 " + (System.currentTimeMillis() - time));

		// Derive the new statements
		// Iterator<Long> itr2 = derivedProps.iterator();
		Iterator<Entry<Long, Long>> itr2 = derivedProps.iterator(); // Modified
																	// by
																	// WuGang,
																	// 2010-08-26
		oTriple.setSubject(uri);
		oTriple.setPredicate(TriplesUtils.RDF_TYPE);
		oTriple.setObjectLiteral(false);
		while (itr2.hasNext()) {
			// oTriple.setObject(itr2.next());
			Entry<Long, Long> entry = itr2.next();
			oTriple.setObject(entry.getKey()); // Modified by WuGang, 2010-08-26
			// Added by WuGang, 2010-08-26
			long propURI = entry.getValue();
			oTriple.setRpredicate(propURI >> 1); // Modified by WuGang
													// 2010-12-03����RDFSSubPropDomRangeMapper�������ˣ����ڱ��������ƻ���
			if ((propURI & 0x1) == 1) { // Rule 3, for range
				oTriple.setType(TriplesUtils.RDFS_3);
				oTriple.setRsubject(uri_opposite);
				oTriple.setRobject(uri);
			} else { // Rule 2, for domain
				oTriple.setType(TriplesUtils.RDFS_2);
				oTriple.setRsubject(uri);
				oTriple.setRobject(uri_opposite);
			}

			context.getCounter("RDFS derived triples", "subproperty of member").increment(1);
			
			CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 

//			 logger.info("write " + (System.currentTimeMillis() - time));
			time = System.currentTimeMillis();
//			System.out.println("finish " + (System.currentTimeMillis() - time));
			// CassandraDB.writealltripleToMapReduceContext(oTriple, source,
			// context);
			// context.write(source, oTriple);
			// _output.write(conf.get(CassandraDB.COLUMNFAMILY_ALLTRIPLES),
			// ByteBufferUtil.bytes(key.toString()),
			// Collections.singletonList(m));
			// Reporter reporter = null ;
			// _output.getCollector(CassandraDB.COLUMNFAMILY_ALLTRIPLES,
			// reporter).collect(key, arg1);;
		}

		// logger.info("  " + (System.currentTimeMillis() - time));
		context.getCounter("RDFS derive triples",
				"subprop range and domain rule").increment(derivedProps.size());
		// logger.info("finish " + (System.currentTimeMillis() - time));
		// Mutation m = new Mutation();

	}
	
	
	@Override
	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.
//		outputKey = ByteBufferUtil.bytes(context.getConfiguration().get(CassandraDB.COLUMNFAMILY_ALLTRIPLES));
		try{
			CassandraDB db = new CassandraDB();
			if (domainSchemaTriples == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_DOMAIN_PROPERTY);
				domainSchemaTriples = db.loadMapIntoMemory(filters);
			}

			if (rangeSchemaTriples == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_RANGE_PROPERTY);
				rangeSchemaTriples = db.loadMapIntoMemory(filters);
			}
			db.CassandraDBClose();
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
