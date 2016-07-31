package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

public class RDFSSubPropInheritMapper extends Mapper<Long, Row, BytesWritable, LongWritable> {
	
	private static Logger log = LoggerFactory.getLogger(RDFSSubPropInheritMapper.class);
	protected HashMap<Long, Integer> subpropSchemaTriples = null;

	protected LongWritable oValue = new LongWritable(0);
	byte[] bKey = new byte[17];
	protected BytesWritable oKey = new BytesWritable();
	
	private boolean hasSchemaChanged = false;
	private int previousExecutionStep = -1;
	
	protected void map(Long key, Row row, Context context) throws IOException, InterruptedException {
		int step = row.getInt(CassandraDB.COLUMN_INFERRED_STEPS);
		if (!hasSchemaChanged && step <= previousExecutionStep)
			return;
		
		//update 是否需要增量更新
		if (CassandraDB.updatelabel == 1) {
			CassandraDB.addedtriple1 = false;
//			CassandraDB.addedtriple2 = false;  //先 setup
			int label = row.getInt(CassandraDB.COLUMN_UPDATELABEL);
			if (label == 1) {
				CassandraDB.addedtriple1 = true;
			}			
		}		
	
		long sub = row.getLong(CassandraDB.COLUMN_SUB);
		long pre = row.getLong(CassandraDB.COLUMN_PRE);
		long obj = row.getLong(CassandraDB.COLUMN_OBJ);
		boolean isObjectLiteral = row.getBool(CassandraDB.COLUMN_IS_LITERAL);
		
		//Check if the triple is a subprop inheritance
		if (subpropSchemaTriples.keySet().contains(pre)) {		
			if (subpropSchemaTriples.get(pre) == 1) {
				CassandraDB.addedtriple2 = true;
				CassandraDB.stopReasoner();
			}
			if (CassandraDB.stopreasoner == true) {
				CassandraDB.stopreasoner = false;
				return;
			}
			
			if (!isObjectLiteral)
				bKey[0] = 2;
			else
				bKey[0] = 3;
			
			long time = System.nanoTime();
			NumberUtils.encodeLong(bKey, 1, sub);
			time = System.nanoTime() - time;
			
			time = System.nanoTime();
			NumberUtils.encodeLong(bKey, 9, obj);
			time = System.nanoTime() - time;
			
			oKey.set(bKey, 0, 17);
			oValue.set(pre);
			context.write(oKey, oValue);
//			System.out.println(" i " + i);
		}
				
		//Check suprop transitivity
		if (pre == TriplesUtils.RDFS_SUBPROPERTY && subpropSchemaTriples.containsKey(obj)) {
			//Write the 05 + subject
			
			if (subpropSchemaTriples.get(obj) == 1) {
				CassandraDB.addedtriple2 = true;
				CassandraDB.stopReasoner();
			}
			if (CassandraDB.stopreasoner == true) {
				CassandraDB.stopreasoner = false;
				return;
			}
			
			bKey[0] = 5;
			NumberUtils.encodeLong(bKey, 1, sub);
			oKey.set(bKey, 0, 9);
			oValue.set(obj);
			context.write(oKey, oValue);
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException {
		hasSchemaChanged = false;
		previousExecutionStep = context.getConfiguration().getInt("lastExecution.step", -1);

		if (subpropSchemaTriples == null) {
			subpropSchemaTriples = new HashMap<>();
			try {
				CassandraDB db = new CassandraDB();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBPROPERTY);
				hasSchemaChanged = db.loadSetIntoMemory(subpropSchemaTriples, filters, previousExecutionStep);
				db.CassandraDBClose();
			} catch (TException e) {
				e.printStackTrace();
			}
		} else {
			log.debug("Subprop schema triples already loaded in memory");
		}

				
	}
	
}
