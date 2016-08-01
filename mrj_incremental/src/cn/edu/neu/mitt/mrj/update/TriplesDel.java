package cn.edu.neu.mitt.mrj.update;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.stringtemplate.v4.compiler.STParser.element_return;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.importtriples.FilesImportTriples;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class TriplesDel {
	private static  Session session;
	private static Session getsession(){		
	    Cluster cluster = Cluster.builder()
	            .addContactPoints(CassandraDB.DEFAULT_HOST)
	            .build();
	    Session session = cluster.connect(CassandraDB.KEYSPACE);
		return session;
	}
	
	private static Triple getTriple(){	
 	    String cqlStatement = "SELECT sub, pre, obj FROM justifications WHERE v1 =  or";
	
		return null;
	}
	
	public static void main(String[] args){
		
		Configuration conf = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(conf);
			Path deledir= new Path("/result");
	        boolean isDeleted=hdfs.delete(deledir,true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}	
		try {
			args[11] = "2";
			FilesImportTriples.main(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		CassandraDB.updatelabel = -1;

		Long sub = null, pre = null, obj = null;

		session= getsession();
		Set<Long> eleT = new HashSet<Long>();
 	    String cqlStatement = "SELECT sub, pre, obj FROM justifications WHERE updatelabel = -1";
	    for (Row row : session.execute(cqlStatement)) {
	    	sub = row.getLong(CassandraDB.COLUMN_SUB);
	    	pre = row.getLong(CassandraDB.COLUMN_SUB);
	    	obj = row.getLong(CassandraDB.COLUMN_SUB);
	    	//原始
	    	//先按只有一条要删除的算。
	    	
	    	
	    	
	    	eleT.add(row.getLong("sub"));
	    	eleT.add(row.getLong("pre"));
	    	eleT.add(row.getLong("obj"));
	    }
	    
	    
	    
//	    TSocket socket = new TSocket(CassandraDB.DEFAULT_HOST, Integer.parseInt(CassandraDB.DEFAULT_PORT));
//	    TTransport trans = new TFramedTransport(socket);
//	    try {
//			trans.open();
//		} catch (TTransportException e) {
//			e.printStackTrace();
//		}
//	    TProtocol protocol = new TBinaryProtocol(trans);
//	    Cassandra.Client client =  new Cassandra.Client(protocol);
//	    String query = "SELECT * FROM justifications WHERE updatelabel = -1";
//		try {
//			CqlPreparedResult preparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE);
//		} catch (InvalidRequestException e) {
//			e.printStackTrace();
//		} catch (TException e) {
//			e.printStackTrace();
//		}
	
		
	}

}
