package cn.edu.neu.mitt.mrj.reasoner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class ReasonedJustificationsReducer extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>>{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
		for (IntWritable value : values) {
			//Prepare the insert keys collection
			String[] splitkeys = key.toString().split("_");
			Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
			keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(Long.parseLong(splitkeys[0])));
			keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(Long.parseLong(splitkeys[1])));
			keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(Long.parseLong(splitkeys[2])));
	       //bool
			keys.put(CassandraDB.COLUMN_IS_LITERAL, Boolean.valueOf(splitkeys[3])?ByteBuffer.wrap(new byte[]{1}):ByteBuffer.wrap(new byte[]{0}));
			keys.put(CassandraDB.COLUMN_TRIPLE_TYPE, ByteBufferUtil.bytes(Integer.parseInt(splitkeys[4])));
			keys.put(CassandraDB.COLUMN_RULE, ByteBufferUtil.bytes(Integer.parseInt(splitkeys[5])));
			keys.put(CassandraDB.COLUMN_V1, ByteBufferUtil.bytes(Long.parseLong(splitkeys[6])));
			keys.put(CassandraDB.COLUMN_V2, ByteBufferUtil.bytes(Long.parseLong(splitkeys[7])));
			keys.put(CassandraDB.COLUMN_V3, ByteBufferUtil.bytes(Long.parseLong(splitkeys[8])));
			
			//prepare the insert variables collection
			List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
			int var_inferredsteps = Integer.parseInt(value.toString());
			variables.add(ByteBufferUtil.bytes(var_inferredsteps));
			int var_transitivelevel = Integer.parseInt(splitkeys[9]);
			variables.add(ByteBufferUtil.bytes(var_transitivelevel));
			context.write(keys, variables);
		}

	}

}
