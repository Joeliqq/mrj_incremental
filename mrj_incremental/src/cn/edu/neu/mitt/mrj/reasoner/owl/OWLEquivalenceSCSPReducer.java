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

public class OWLEquivalenceSCSPReducer extends Reducer<LongWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

	protected static Logger log = LoggerFactory.getLogger(OWLEquivalenceSCSPReducer.class);
	
	private TripleSource source = new TripleSource();
	private Triple triple = new Triple();

	protected static  Map<Long, Collection<Long>> subpropSchemaTriples = null;
	public static Map<Long, Collection<Long>> subclassSchemaTriples = null;
	public static Map<Long, Collection<Long>> equivalenceClassesSchemaTriples = null;		// Added by WuGang
	public static Map<Long, Collection<Long>> equivalencePropertiesSchemaTriples = null;		// Added by WuGang
	
	public Set<Long> equivalenceClasses = new HashSet<Long>();
	public Set<Long> superClasses = new HashSet<Long>();

	public Set<Long> equivalenceProperties = new HashSet<Long>();
	public Set<Long> superProperties = new HashSet<Long>();
	
	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		
		equivalenceClasses.clear();
		superClasses.clear();
		equivalenceProperties.clear();
		superProperties.clear();
		
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			byte[] bValue = value.getBytes();
			long resource = NumberUtils.decodeLong(bValue, 1);
			switch (bValue[0]) {
			case 0:
				superClasses.add(resource);
				break;
			case 1:
				superProperties.add(resource);
				break;
			case 2:
				equivalenceClasses.add(resource);
				break;
			case 3:
				equivalenceProperties.add(resource);
				break;
			default:
				break;
			}
		}
		
		//Equivalence classes
		Iterator<Long> itr2 = equivalenceClasses.iterator();
		while (itr2.hasNext()) {
			long resource = itr2.next();
			boolean found = false;
			Collection<Long> existingClasses = subclassSchemaTriples.get(key.get());
			if (existingClasses != null) {
				Iterator<Long> exsItr = existingClasses.iterator();
				while (exsItr.hasNext() && !found) {
					if (exsItr.next() == resource)
						found = true;
				}
			}
			
			if (!found) {	// ��������ó��Ľ��
				triple.setObject(resource);
				triple.setSubject(key.get());
				triple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
				
				// Added by WuGang
				if (equivalenceClassesSchemaTriples.containsKey(key.get())){
					triple.setType(TriplesUtils.OWL_HORST_12a);
					triple.setRsubject(triple.getSubject());
					triple.setRpredicate(TriplesUtils.OWL_EQUIVALENT_CLASS);
					triple.setRobject(triple.getObject());
				}else{
					triple.setType(TriplesUtils.OWL_HORST_12b);
					triple.setRsubject(triple.getObject());
					triple.setRpredicate(TriplesUtils.OWL_EQUIVALENT_CLASS);
					triple.setRobject(triple.getSubject());
				}
//				context.write(source, triple);
				CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
			}
		}
		
		//Equivalence properties
		itr2 = equivalenceProperties.iterator();
		while (itr2.hasNext()) {
			long resource = itr2.next();
			boolean found = false;
			Collection<Long> existingClasses = subpropSchemaTriples.get(key.get());
			if (existingClasses != null) {
				Iterator<Long> exsItr = existingClasses.iterator();
				while (exsItr.hasNext() && !found) {
					if (exsItr.next() == resource)
						found = true;
				}
			}
			
			if (!found) {
				triple.setObject(resource);
				triple.setSubject(key.get());
				triple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
				
				// Added by WuGang
				if (equivalencePropertiesSchemaTriples.containsKey(key.get())){
					triple.setType(TriplesUtils.OWL_HORST_13a);
					triple.setRsubject(triple.getSubject());
					triple.setRpredicate(TriplesUtils.OWL_EQUIVALENT_PROPERTY);
					triple.setRobject(triple.getObject());
				}else{
					triple.setType(TriplesUtils.OWL_HORST_13b);
					triple.setRsubject(triple.getObject());
					triple.setRpredicate(TriplesUtils.OWL_EQUIVALENT_PROPERTY);
					triple.setRobject(triple.getSubject());
				}
				
//				context.write(source, triple);
				CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
			}
		}

		//Subproperties
		// Modified by WuGang,����ò��Ӧ����superProperties
//		itr2 = equivalenceProperties.iterator();
		itr2 = superProperties.iterator();
		while (itr2.hasNext()) {
			long resource = itr2.next();
			boolean found = false;
			Collection<Long> existingClasses = subpropSchemaTriples.get(resource);
			if (existingClasses != null) {
				Iterator<Long> exsItr = existingClasses.iterator();
				while (exsItr.hasNext() && !found) {
					if (exsItr.next() == key.get())
						found = true;
				}
			}
			
			if (found && !equivalenceProperties.contains(resource)) {
				triple.setSubject(key.get());
				triple.setPredicate(TriplesUtils.OWL_EQUIVALENT_PROPERTY);
				// Modified by WuGang
//				triple.setPredicate(resource);	
				triple.setObject(resource);
				
				// Added by WuGang
				triple.setType(TriplesUtils.OWL_HORST_13c);
				triple.setRsubject(triple.getSubject());
				triple.setRpredicate(TriplesUtils.RDFS_SUBPROPERTY);
				triple.setRobject(triple.getObject());
				
//				context.write(source, triple);
				CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
			}
		}
		
		//Subclasses
		// Modified by WuGang,����ò��Ӧ����superClasses
//		itr2 = equivalenceClasses.iterator();
		itr2 = superClasses.iterator();
		while (itr2.hasNext()) {
//			System.out.println("In OWLEquivalenceSCSPMapper, processing 12c ");
			long resource = itr2.next();
			boolean found = false;
			Collection<Long> existingClasses = subclassSchemaTriples.get(resource);
			if (existingClasses != null) {
				Iterator<Long> exsItr = existingClasses.iterator();
				while (exsItr.hasNext() && !found) {
					if (exsItr.next() == key.get())
						found = true;
				}
			}
			
			if (found && !equivalenceClasses.contains(resource)) {
				triple.setSubject(key.get());
				triple.setPredicate(TriplesUtils.OWL_EQUIVALENT_CLASS);
				// Modified by WuGang
//				triple.setPredicate(resource);
				triple.setObject(resource);
				
				// Added by WuGang
				triple.setType(TriplesUtils.OWL_HORST_12c);
				triple.setRsubject(triple.getSubject());
				triple.setRpredicate(TriplesUtils.RDFS_SUBCLASS);
				triple.setRobject(triple.getObject());
//				context.write(source, triple);
				CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

		source.setDerivation(TripleSource.OWL_DERIVED);
		source.setStep((byte)context.getConfiguration().getInt("reasoner.step", 0));
		triple.setObjectLiteral(false);
		
		CassandraDB db;
		try{
			db = new CassandraDB();
			if (subpropSchemaTriples == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBPROPERTY);
				subpropSchemaTriples = db.loadMapIntoMemory(filters);
			}

			if (subclassSchemaTriples == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBCLASS);
				subclassSchemaTriples = db.loadMapIntoMemory(filters);
			}

			// Added by WuGang
			if (equivalenceClassesSchemaTriples == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_EQUIVALENT_CLASS);
				equivalenceClassesSchemaTriples = db.loadMapIntoMemory(filters);
			}

			// Added by WuGang
			if (equivalencePropertiesSchemaTriples == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_EQUIVALENT_PROPERTY);
				equivalencePropertiesSchemaTriples = db.loadMapIntoMemory(filters);
			}
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
