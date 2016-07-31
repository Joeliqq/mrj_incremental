package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class OWLAllSomeValuesReducer extends Reducer<BytesWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	
	protected static Logger log = LoggerFactory.getLogger(OWLAllSomeValuesReducer.class);
	private Triple triple = new Triple();
	private TripleSource source = new TripleSource();
	
	private LinkedList<Long> types = new LinkedList<Long>();
	private LinkedList<Long> resources = new LinkedList<Long>();
	
	// Added by WuGang
	private LinkedList<Long> others = new LinkedList<Long>();	// ��types����һ��
	private LinkedList<Byte> s_a_types = new LinkedList<Byte>();	// ��types����һ��,���ڴ洢��someValues(0)����allValues(1)����

	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		//log.info("I'm in OWLAllSomeValuesReducer");
		
		types.clear();
		resources.clear();
		
		byte[] bKey = key.getBytes();
		long rSubject = NumberUtils.decodeLong(bKey, 9);	// rSubject����key�ĵڶ���Long����ʼλ��Ϊ9���ͷ����һ��byte��
		long predicate = NumberUtils.decodeLong(bKey, 1);	// Added by WuGang 2010-07-14
		
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			byte[] bValue = value.getBytes();
			if (bValue[0] == 1) { //Type triple
				types.add(NumberUtils.decodeLong(bValue, 1));
				others.add(NumberUtils.decodeLong(bValue, 9));	// Added by WuGang, ��types����Ҫ���⴫��һ��long�ͣ���һ��byte
				s_a_types.add(bValue[17]);
			} else { //Resource triple
				resources.add(NumberUtils.decodeLong(bValue, 1));
			}
		}
		
		if (types.size() > 0 && resources.size() > 0) {
			
			if (types.size() > 100000 || resources.size() > 100000)
			log.debug("Size type: " + types.size() + " size resources " + resources.size());
			
//			System.out.println("Begin to output justification graph.");
			
			Iterator<Long> itrResource = resources.iterator();
			while (itrResource.hasNext()) {
				long resource = itrResource.next();
				triple.setSubject(resource);
				// ����Types���͵�value����someValuesΪ����((p,x),(v,w))����allValuesΪ����((p,w),(u,v))				
				Iterator<Long> itrTypes = types.listIterator();
				Iterator<Long> itrOthers = others.listIterator();
				Iterator<Byte> itrSATypes = s_a_types.listIterator();
				while (itrTypes.hasNext()) {
					long type = itrTypes.next();
					triple.setObject(type);
					
					// Added by WuGang����triple��ֵ
					long other = itrOthers.next();
					byte s_a_type = itrSATypes.next();
					triple.setRsubject(rSubject);	// ��someValues������x,��allValues������w
					// Modified by WuGang 2010-07-14
//					triple.setRpredicate(TriplesUtils.RDF_TYPE);	//rdf:type
					triple.setRpredicate(predicate);
					triple.setRobject(other); // ��someValues������w,��allValues������v
					switch (s_a_type) {
					case 0:
						triple.setType(TriplesUtils.OWL_HORST_15);
						break;
					case 1:
						triple.setType(TriplesUtils.OWL_HORST_16);
						break;
					}
					


//					System.out.println("Generate an extended triple for OWLAllSomeValues: " + triple);
//					context.write(source, triple);
					CassandraDB.writeJustificationToMapReduceContext(triple, source, context); 		
				}
			}
		}
	}

	@Override
	public void setup(Context context) {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

		source.setDerivation(TripleSource.OWL_DERIVED);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
		triple.setObjectLiteral(false);
		triple.setPredicate(TriplesUtils.RDF_TYPE);
	}

	@Override
	protected void cleanup(
			Reducer<BytesWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
}
