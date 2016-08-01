package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

public class OWLHasValueMapper extends Mapper<Long, Row, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory.getLogger(OWLHasValueMapper.class);
	
	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	private byte[] values = new byte[17];
	
	private HashMap<Long, Integer> hasValue = null;
	private HashMap<Long, Integer> hasValueInverted = null;
	private HashMap<Long, Integer> onProperty = null;
	private HashMap<Long, Integer> onPropertyInverted = null;
	private int previousStep = -1;
	
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
		if (CassandraDB.updatelabel == 1) {
			CassandraDB.addedtriple1 = false;
			int label = row.getInt(CassandraDB.COLUMN_UPDATELABEL);
			if (label == 1) {
				CassandraDB.addedtriple1 = true;
			}			
		}
		int step = row.getInt(CassandraDB.COLUMN_INFERRED_STEPS);
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

//		System.out.println("In OWLHasValueMapper: key.getStep()="+key.getStep()+"; previousStep="+previousStep);
		
		if (step <= previousStep) {
			return;
		} //TODO: check whether also the schema is modified

		oKey.set(value.getSubject());
		if (value.getPredicate() == TriplesUtils.RDF_TYPE &&	// ����14b������value����(u rdf:type v)��Ŀ������reduce������(u p w)�����ǻ�Ҫ���14b(v owl:hasValue w)
				hasValue.containsKey(value.getObject()) &&
				onProperty.containsKey(value.getObject())) {
			
			if (hasValue.get(value.getObject()) == 1 || onProperty.get(value.getObject()) == 1) {
				CassandraDB.addedtriple2 = true;
				CassandraDB.stopReasoner();
			}
			if (CassandraDB.stopreasoner == true) {
				CassandraDB.stopreasoner = false;
				return;
			}
			
//			System.out.println("In OWLHasValueMapper for 14b: " + value);	// Added by Wugang
			values[0] = 0;
			NumberUtils.encodeLong(values, 1, value.getObject());
			oValue.set(values, 0, 9);
			
			context.write(oKey, oValue);
		} else if (value.getPredicate() != TriplesUtils.RDF_TYPE	// ����14a������value����(u p w)��Ŀ������reduce������(u rdf:type v)�����ǻ�Ҫ���14a(v owl:hasValue w)
				&& hasValueInverted.containsKey(value.getObject())
				&& onPropertyInverted.containsKey(value.getPredicate())) {
			
			if (hasValueInverted.get(value.getObject()) == 1 || onPropertyInverted.get(value.getPredicate()) == 1) {
				CassandraDB.addedtriple2 = true;
				CassandraDB.stopReasoner();
			}
			if (CassandraDB.stopreasoner == true) {
				CassandraDB.stopreasoner = false;
				return;
			}
			
//			System.out.println("In OWLHasValueMapper for 14a: " + value);	// Added by Wugang
			values[0] = 1;
			NumberUtils.encodeLong(values, 1, value.getPredicate());
			NumberUtils.encodeLong(values, 9, value.getObject());
			oValue.set(values, 0, 17);
			
			context.write(oKey, oValue);

		}
		
		// Moved into if-else by WuGang, 20150203
//		context.write(oKey, oValue);
	}

	public void setup(Context context) throws IOException {
		previousStep = context.getConfiguration().getInt("reasoner.previousStep", -1);

		try{
			CassandraDB db = new CassandraDB();
		
			if (hasValue == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.DATA_TRIPLE_HAS_VALUE);
				
				hasValue = new HashMap<>();
				db.loadSetIntoMemory(hasValue, filters, -1, false);
				
				hasValueInverted = new HashMap<>();
				db.loadSetIntoMemory(hasValueInverted, filters, -1, true);
			}
			
			if (onProperty == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_ON_PROPERTY);

				onProperty = new HashMap<>();
				db.loadSetIntoMemory(onProperty, filters, -1, false);
				
				onPropertyInverted = new HashMap<>();
				db.loadSetIntoMemory(onPropertyInverted, filters, -1, true);
			}
			db.CassandraDBClose();
		}catch(TException te){
			te.printStackTrace();
		}
	}
}
