package cn.edu.neu.mitt.mrj.reasoner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

public class ReasonedJustificationsMapper extends Mapper<Long, Row, Text, IntWritable>{
	private Cluster cluster;
	private Session session;
	//**
	public void map(Long keys, Row rows, Context context) throws IOException, InterruptedException{
			
		Integer inferredsteps;
		Integer transitivelevel;
	//	for (Row rows : row){
			if (rows.getInt(CassandraDB.COLUMN_RULE) != 0) {
				
				String conKey;
				//*****
				conKey = rows.getLong(CassandraDB.COLUMN_SUB)	//��ʹ��ByteBufferUtil��
						+ "_" + rows.getLong(CassandraDB.COLUMN_PRE)
						+ "_" + rows.getLong(CassandraDB.COLUMN_OBJ)
						+ "_" + rows.getBool(CassandraDB.COLUMN_IS_LITERAL)
						+ "_" + rows.getInt(CassandraDB.COLUMN_TRIPLE_TYPE)
						+ "_" + rows.getInt(CassandraDB.COLUMN_RULE)
						+ "_" + rows.getLong(CassandraDB.COLUMN_V1)
						+ "_" + rows.getLong(CassandraDB.COLUMN_V2)
						+ "_" + rows.getLong(CassandraDB.COLUMN_V3)
						+ "_" + rows.getInt(CassandraDB.COLUMN_INFERRED_STEPS);			// Modified by WuGang, 2015-07-15
				transitivelevel = rows.getInt(CassandraDB.COLUMN_TRANSITIVE_LEVELS);	// Added by WuGang, 2015-07-15
				
				context.write(new Text(conKey), new IntWritable(transitivelevel));
			}
		//}
		
	}
	
	public void setup(Context context) throws IOException, InterruptedException{	
		cluster = Cluster.builder().addContactPoint(cn.edu.neu.mitt.mrj.utils.Cassandraconf.host).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("-------Connected to cluster: %s\n", metadata.getClusterName());
		session = cluster.connect();

        String query = "CREATE TABLE IF NOT EXISTS " + CassandraDB.KEYSPACE + "."  + "resultrows" + 
                " ( " + 
                CassandraDB.COLUMN_IS_LITERAL + " boolean, " +	// partition key 
                CassandraDB.COLUMN_RULE + " int, " +
                CassandraDB.COLUMN_SUB + " bigint, " +			// partition key
                CassandraDB.COLUMN_TRIPLE_TYPE + " int, " +
                CassandraDB.COLUMN_PRE + " bigint, " +			// partition key
                CassandraDB.COLUMN_OBJ + " bigint, " +			// partition key
                CassandraDB.COLUMN_V1 + " bigint, " +
                CassandraDB.COLUMN_V2 + " bigint, " +
                CassandraDB.COLUMN_V3 + " bigint, " +
				CassandraDB.COLUMN_INFERRED_STEPS + " int, " +		// this is the only field that is not included in the primary key
				CassandraDB.COLUMN_TRANSITIVE_LEVELS + " int, " +
                "   PRIMARY KEY ((" + CassandraDB.COLUMN_IS_LITERAL + ", " + CassandraDB.COLUMN_RULE + ", " + CassandraDB.COLUMN_SUB  + "), " +
                CassandraDB.COLUMN_TRIPLE_TYPE + ", " + CassandraDB.COLUMN_PRE + ", " + CassandraDB.COLUMN_OBJ + ", " + CassandraDB.COLUMN_V1 + ", " + CassandraDB.COLUMN_V2 + ", " +  CassandraDB.COLUMN_V3 +
                //", " + COLUMN_TRIPLE_TYPE +
                " ) ) ";

//        session.execute(query);
//		query = "CREATE INDEX on mrjks.resultrows (sub) ;";
//		session.execute(query);
//		query = "CREATE INDEX on mrjks.resultrows (obj) ;";
//		session.execute(query);
//		query = "CREATE INDEX on mrjks.resultrows (pre) ;";
//		session.execute(query);
//		query = "CREATE INDEX on mrjks.resultrows (isliteral) ;";
//		session.execute(query);

	}
}
