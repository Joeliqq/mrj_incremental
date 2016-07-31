package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class OWLTransitivityMapper extends Mapper<Long, Row, BytesWritable, BytesWritable> {
	
	protected static Logger log = LoggerFactory.getLogger(OWLTransitivityMapper.class);
	byte[] keys = new byte[16];
	private BytesWritable oKey = new BytesWritable();
	byte[] values = new byte[17];
	private BytesWritable oValue = new BytesWritable();
	
	int level = 0;
	int baseLevel = 0;
	int minLevel = 0;
	int maxLevel = 0;
	
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
		//int step = row.getInt(CassandraDB.COLUMN_INFERRED_STEPS);
		int level = row.getInt(CassandraDB.COLUMN_TRANSITIVE_LEVELS);	// Added by WuGang, 2015-07-15
				
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);
		
		byte derivation = TripleSource.TRANSITIVE_ENABLED;	// We now do transitive inference always!!!

		if (value.getSubject() != value.getObject()
				&& !value.isObjectLiteral()) {
		
			if (level == minLevel || level == maxLevel) {
				NumberUtils.encodeLong(keys,0,value.getPredicate());
				NumberUtils.encodeLong(keys,8,value.getObject());
				oKey.set(keys, 0, 16);
				
				if (derivation == TripleSource.TRANSITIVE_DISABLED)
					values[0] = 1;
				else
					values[0] = 0;
				
				NumberUtils.encodeLong(values, 1, level);
				NumberUtils.encodeLong(values, 9, value.getSubject());
				oValue.set(values, 0, 17);
				
				context.write(oKey, oValue);
			}
			
			
			if (level > minLevel) {
				NumberUtils.encodeLong(keys,0,value.getPredicate());
				NumberUtils.encodeLong(keys,8,value.getSubject());
				oKey.set(keys, 0, 16);
				
				if (derivation == TripleSource.TRANSITIVE_DISABLED)
					values[0] = 3;
				else
					values[0] = 2;
				NumberUtils.encodeLong(values, 1, level);
				NumberUtils.encodeLong(values, 9, value.getObject());
				oValue.set(values, 0, 17);
				
				context.write(oKey, oValue);
			}
			
			//����u p w, w p v���������key��(p, w),�����value��(value[0], key.getStep(), value.getObject)
		}
	}

	@Override
	public void setup(Context context) {

		level = context.getConfiguration().getInt("reasoning.transitivityLevel", 0);
		baseLevel = context.getConfiguration().getInt("reasoning.baseLevel", 0) - 1;
		minLevel = Math.max(1, (int)Math.pow(2,level - 2)) + baseLevel;
		maxLevel = Math.max(1, (int)Math.pow(2,level - 1)) + baseLevel;
		if (level == 1)
			minLevel = 0 + baseLevel;
	}
}