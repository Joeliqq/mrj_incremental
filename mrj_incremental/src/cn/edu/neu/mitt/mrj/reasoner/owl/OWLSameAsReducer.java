package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class OWLSameAsReducer extends Reducer<LongWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	
	private TripleSource oKey = new TripleSource();
	private Triple oValue = new Triple();
	private HashSet<Long> duplicates = new HashSet<Long>();
	
	private List<Long> storage = new LinkedList<Long>();

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		
		/* init */
		duplicates.clear();
		storage.clear();
		
//		System.out.println("Once into OWLSameAsReducer...");
		
		oValue.setSubject(key.get());
		boolean foundReplacement = false;
		/* Start to iterate over the values */
		Iterator<BytesWritable> itr = values.iterator();
//		System.out.println("Prepare to processing values");
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			long lValue = NumberUtils.decodeLong(value.getBytes(), 1);
//			System.out.println("processing " + lValue + " with the first byte is: " + value.getBytes()[0]);
			if (value.getBytes()[0] != 0) {	// 1��ÿһ��value����һ����Ա
					//Store in-memory
					storage.add(lValue);
//					System.out.println("Storage size is: " + storage.size());
				//}
			} else {	// 0���ϲ�һ��resource�����ĸ����飨valueֵ��
//				System.out.println("Prepare to repalce: lValue is " + lValue + " and oValue.getSubject() is " + oValue.getSubject());
				if (lValue < oValue.getSubject()) {
//					System.out.println("Hahahahah, I'm here!");
					foundReplacement = true;
					oValue.setSubject(lValue);
				}
			}
		}
		
		//Empty the in-memory data structure
		Iterator<Long> itr2 = storage.iterator();
		while (itr2.hasNext()) {
			long lValue = itr2.next();
			if (!duplicates.contains(lValue)) {
				oValue.setObject(lValue);
				CassandraDB.writeJustificationToMapReduceContext(oValue, oKey, context); 		
				duplicates.add(lValue);
			}		
		}
		if (foundReplacement) { 
//			System.out.println("In OWLSameAsReducer: " + context.getCounter("synonyms", "replacements").getValue());
//			System.out.println("synonyms - replacements: " + storage.size());
			context.getCounter("synonyms", "replacements").increment(storage.size()); 
//			System.out.println("In OWLSameAsReducer: " + context.getCounter("synonyms", "replacements").getValue());
		}
	}

	@Override
	public void setup(Context context) {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

		oValue.setObjectLiteral(false);
		oValue.setPredicate(TriplesUtils.OWL_SAME_AS);
		// Added by WuGang 20150130, 
		//   A special rule for build a synonyms table (make it filterable by tripletype in the future) 
		//   which in fact not belong to OWL HORST.
		oValue.setType(TriplesUtils.OWL_HORST_SYNONYMS_TABLE);	
		
		oKey.setDerivation(TripleSource.OWL_DERIVED);
		oKey.setStep(context.getConfiguration().getInt("reasoner.step", 0));
	}

	@Override
	protected void cleanup(
			Reducer<LongWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
