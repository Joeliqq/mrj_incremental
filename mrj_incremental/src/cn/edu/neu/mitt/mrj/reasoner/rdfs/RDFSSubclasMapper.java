package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

public class RDFSSubclasMapper extends Mapper<Long, Row, BytesWritable, LongWritable> {
	
	protected static Logger log = LoggerFactory.getLogger(RDFSSubclasMapper.class);

	byte[] bKey = new byte[9];
	protected BytesWritable oKey = new BytesWritable();
	protected LongWritable oValue = new LongWritable(0);

	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

		oValue.set(value.getObject());
		if (value.getPredicate() == TriplesUtils.RDF_TYPE) {
			bKey[0] = 0;
		} else { //It's a subclass file
			bKey[0] = 1;
		}
		NumberUtils.encodeLong(bKey,1,value.getSubject());
		oKey.set(bKey, 0, 9);
		context.write(oKey, oValue);
		
//		System.out.println("׼����RDFSSubclasMapper-"+value);
	}
	
	protected void setup(Context context) throws IOException, InterruptedException{
		
	}
//	protected void cleanup(Context context) throws IOException, InterruptedException{
//		try {
//			CassandraDB db = new CassandraDB();
//			db.UnIndex();
//			db.CassandraDBClose();
//		} catch (Exception e) {
//			// TODO: handle exception
//		}
//	}
	
}
