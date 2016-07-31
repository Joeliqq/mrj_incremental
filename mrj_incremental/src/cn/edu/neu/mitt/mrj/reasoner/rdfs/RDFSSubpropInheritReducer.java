package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;
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

import com.sun.corba.se.spi.ior.Writeable;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

public class RDFSSubpropInheritReducer extends Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	
	private static Logger log = LoggerFactory.getLogger(RDFSSubpropInheritReducer.class);
	
	protected static  Map<Long, Collection<Long>> subpropSchemaTriples = null;
	protected Set<Long> propURIs = new HashSet<Long>();
	protected Set<Long> derivedProps = new HashSet<Long>();
	private TripleSource source = new TripleSource();

	private Triple oTriple = new Triple();
	private Triple oTriple2 = new Triple();
	
	private Map<String, ByteBuffer> keys = 	new LinkedHashMap<String, ByteBuffer>();
	private Map<String, ByteBuffer> allkeys = 	new LinkedHashMap<String, ByteBuffer>();
	private List<ByteBuffer> allvariables =  new ArrayList<ByteBuffer>();
	private List<ByteBuffer> allTValues =  new ArrayList<ByteBuffer>();
	private List<ByteBuffer> stepsValues =  new ArrayList<ByteBuffer>();

	private void recursiveScanSubproperties(long value, Set<Long> set) {
		Collection<Long> subprops = subpropSchemaTriples.get(value);
		if (subprops != null) {
			Iterator<Long> itr = subprops.iterator();
			while (itr.hasNext()) {
				long subprop = itr.next();
				if (!set.contains(subprop)) {
					set.add(subprop);
					recursiveScanSubproperties(subprop, set);
				}
			}
		}
	}
	
	@Override
	public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
											throws IOException, InterruptedException {

		byte[] bKey = key.getBytes();
		switch(bKey[0]) {
		case 2:
		case 3:	// rdfs rule 7
			//subprop inheritance
			long subject = NumberUtils.decodeLong(bKey, 1);
			long uri = NumberUtils.decodeLong(bKey, 9);
			propURIs.clear();
			//filter the properties that are already present
			Iterator<LongWritable> itr = values.iterator();
			/*
			 * values在使用iterator之后会将值清空，使用list记录values
			 */
			List<Long> list1 = new ArrayList<Long>();
			while (itr.hasNext()) {
				long value = itr.next().get();
				list1.add(value);
				if (!propURIs.contains(value)) {
					recursiveScanSubproperties(value, propURIs);
				}
				
			}
			
			Iterator<Long> itr3 = propURIs.iterator();
			boolean isLiteral = bKey[0] == 3;
			oTriple.setSubject(subject);
			oTriple.setObject(uri);
			oTriple.setObjectLiteral(isLiteral);
			// Added by WuGang, 2010-08-26
			oTriple.setType(TriplesUtils.RDFS_7);
			oTriple.setRsubject(subject);
			oTriple.setRobject(uri);
//			while (itr3.hasNext()) {
//				oTriple.setPredicate(itr3.next());				
//				context.write(source, oTriple);
//			}
			// Modified by WuGang, 2010-08-26
			while (itr3.hasNext()) {
				oTriple.setPredicate(itr3.next());				
				for (Long pre : list1) {
					oTriple.setRpredicate(pre);
					context.getCounter("RDFS derived triples", "subproperty of member").increment(1);
				}
			}

			context.getCounter("RDFS derived triples", "subprop inheritance rule").increment(propURIs.size());
			break;
		case 5:	// rdfs rule 5
			//Subproperty transitivity
			subject = NumberUtils.decodeLong(bKey, 1);
			propURIs.clear();
			//filter the properties that are already present
			Iterator<LongWritable> itr2 = values.iterator();
			List<Long> list2 = new ArrayList<Long>();
			while (itr2.hasNext()) {
				long value = itr2.next().get();
				list2.add(value);
				if (!propURIs.contains(value)) {
					recursiveScanSubproperties(value, propURIs);
				}
			}
			
			Iterator<Long> itr4 = propURIs.iterator();
			oTriple.setSubject(subject);
			oTriple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
			oTriple.setObjectLiteral(false);
			// Added by WuGang, 2010-08-26
			oTriple.setType(TriplesUtils.RDFS_5);
			oTriple.setRsubject(subject);
			oTriple.setRpredicate(TriplesUtils.RDFS_SUBPROPERTY);
//			while (itr4.hasNext()) {
//				oTriple.setObject(itr4.next());				
//				context.write(source, oTriple);
//			}
			// Modified by WuGang, 2010-08-26

			while (itr4.hasNext()) {
				oTriple.setObject(itr4.next());
				for(Long obj:list2){
					oTriple.setRobject(obj);
					context.getCounter("RDFS derived triples", "subproperty of member").increment(1);
//					context.write(source, oTriple);
				}

			}

			context.getCounter("RDFS derived triples", "subprop transitivity rule").increment(propURIs.size());

			break;
		default: 
			break;
		}
		
	}

	@Override
	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

		if (subpropSchemaTriples == null) {
			CassandraDB db;
			try {
				db = new CassandraDB();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBPROPERTY);
				subpropSchemaTriples = db.loadMapIntoMemory(filters);
//				subpropSchemaTriples = FilesTriplesReader.loadMapIntoMemory("FILTER_ONLY_SUBPROP_SCHEMA", context);
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
		} else {
			log.debug("Subprop schema triples already loaded in memory");
		}

		source.setDerivation(TripleSource.RDFS_DERIVED);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));

		oTriple2.setPredicate(TriplesUtils.RDF_TYPE);
		oTriple2.setObjectLiteral(false);		

		
	}

	@Override
	protected void cleanup(
			Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		/*
		 * 不写close就会写不进数据库。
		 */

		super.cleanup(context);
	}
	
	
	
}
