package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class RDFSSpecialPropsReducer extends Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

	private TripleSource source = new TripleSource();
	private Triple oTriple = new Triple();
	private Map<String, ByteBuffer> keys = 	new LinkedHashMap<String, ByteBuffer>();
	private Map<String, ByteBuffer> allkeys = 	new LinkedHashMap<String, ByteBuffer>();
	private List<ByteBuffer> allvariables =  new ArrayList<ByteBuffer>();
	private List<ByteBuffer> allTValues =  new ArrayList<ByteBuffer>();
	private List<ByteBuffer> stepsValues =  new ArrayList<ByteBuffer>();

	@Override
	public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		byte[] bKey = key.getBytes();
		Iterator<LongWritable> itr = values.iterator();
		

		
		while (itr.hasNext()) {
			long value = itr.next().get();
			if (value == TriplesUtils.RDFS_LITERAL && (bKey[0] == 0 || bKey[0] == 2))
				return;
			else if (value == TriplesUtils.RDFS_MEMBER && (bKey[0] == 1 || bKey[0] == 4 || bKey[0] == 5))
				return;
			else if (value == TriplesUtils.RDFS_RESOURCE && bKey[0] == 3)
				return;
		}

		switch(bKey[0]) {
		/*case 0:
			oTriple.setSubject(NumberUtils.decodeNumber(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDF_TYPE);
			oTriple.setPredicate(TriplesUtils.RDFS_LITERAL);
			oTriple.setObjectLiteral(false);
			output.collect(source, oTriple);
			break;*/
		case 1:	// Rule 12
			oTriple.setSubject(NumberUtils.decodeLong(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
//			oTriple.setPredicate(TriplesUtils.RDFS_MEMBER);
			oTriple.setObject(TriplesUtils.RDFS_MEMBER);	// Modified by WuGang, 2010-08-26
			oTriple.setObjectLiteral(false);
			// Added by WuGang, 2010-08-26
			oTriple.setType(TriplesUtils.RDFS_12);
			oTriple.setRsubject(oTriple.getSubject());
			oTriple.setRpredicate(TriplesUtils.RDF_TYPE);
			oTriple.setRobject(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY);
			context.getCounter("RDFS derived triples", "subproperty of member").increment(1);

			CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 		
			break;
		case 2:	// Rule 13
			oTriple.setSubject(NumberUtils.decodeLong(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
//			oTriple.setPredicate(TriplesUtils.RDFS_LITERAL);
			oTriple.setObject(TriplesUtils.RDFS_LITERAL);	// Modified by WuGang, 2010-08-26
			oTriple.setObjectLiteral(false);
			// Added by WuGang, 2010-08-26
			oTriple.setType(TriplesUtils.RDFS_13);
			oTriple.setRsubject(oTriple.getSubject());
			oTriple.setRpredicate(TriplesUtils.RDF_TYPE);
			oTriple.setRobject(TriplesUtils.RDFS_DATATYPE);
			CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 
			//			context.write(source, oTriple);
			context.getCounter("RDFS derived triples", "subclass of literal").increment(1);
			break;
		case 3:	// Rule 8
			oTriple.setSubject(NumberUtils.decodeLong(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
//			oTriple.setPredicate(TriplesUtils.RDFS_RESOURCE);
			oTriple.setObject(TriplesUtils.RDFS_RESOURCE);	// Modified by WuGang, 2010-08-26
			oTriple.setObjectLiteral(false);
			// Added by WuGang, 2010-08-26
			oTriple.setType(TriplesUtils.RDFS_8);
			oTriple.setRsubject(oTriple.getSubject());
			oTriple.setRpredicate(TriplesUtils.RDF_TYPE);
			oTriple.setRobject(TriplesUtils.RDFS_CLASS);
			context.getCounter("RDFS derived triples", "subclass of resource").increment(1);
			CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 		
			//context.write(source, oTriple);
			break;
		case 4:	// û�ж�Ӧ��rdfs rule��
		case 5:	// û�ж�Ӧ��rdfs rule��
			oTriple.setSubject(NumberUtils.decodeLong(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDFS_MEMBER);
//			oTriple.setPredicate(NumberUtils.decodeLong(bKey, 9));
			oTriple.setPredicate(NumberUtils.decodeLong(bKey, 9));	// Modified by WuGang, 2010-08-26
			if (bKey[0] == 4)
				oTriple.setObjectLiteral(false);
			else
				oTriple.setObjectLiteral(true);
			context.getCounter("RDFS derived triples", "subproperty inheritance of member").increment(1);
			CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context); 		
//			context.write(source, oTriple);
		default: 
			break;
		}

	}

	@Override
	public void setup(Context context) {		
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.		

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
