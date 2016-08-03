package cn.edu.neu.mitt.mrj.update;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
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
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.Select;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.importtriples.FilesImportTriples;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.justification.OWLHorstJustification;
import cn.edu.neu.mitt.mrj.reasoner.Experiments;

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
	
	private static void delOriginalTriple(Session session, Long sub, Long pre, Long obj){
 	    String cqlStatement = "DELETE FROM " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS + " WHERE sub = " + sub + " AND pre = " + pre + " AND obj = " + obj;
 	    session.execute(cqlStatement);
	}
	
	public static void createIndexOnRSPO(Session session){
		session.execute("CREATE INDEX on justifications (v1)");
		session.execute("CREATE INDEX on justifications (v2)");
		session.execute("CREATE INDEX on justifications (v3)");
	}
	
	/*
	 * 得到所有可能包含Triple的大集合，用于做justification展开
	 * r1  是sub obj 和 pre
	 */
	private static HashSet<Triple> getTriples(Long r1){
		ResultSet resultSet1 = session.execute("SELECT sub ,pre ,obj  FROM mrjks.resultrows WHERE v1 = " + r1);
		ResultSet resultSet2 = session.execute("SELECT sub ,pre ,obj  FROM mrjks.resultrows WHERE v2 = " + r1);
		ResultSet resultSet3 = session.execute("SELECT sub ,pre ,obj  FROM mrjks.resultrows WHERE v2 = " + r1);

		//
//		TupleType theType = TupleType.of(DataType.bigint(), DataType.bigint(), DataType.bigint());
		HashSet<Triple> tripleSet = new HashSet<Triple>();
		
		for(Row row : resultSet1){
			Triple triple = null;
			triple.setSubject(row.getLong(CassandraDB.COLUMN_SUB));
			triple.setObject(row.getLong(CassandraDB.COLUMN_PRE));
			triple.setPredicate(row.getLong(CassandraDB.COLUMN_OBJ));
			tripleSet.add(triple);
		}
		for(Row row : resultSet2){
			Triple triple = null;
			triple.setSubject(row.getLong(CassandraDB.COLUMN_SUB));
			triple.setObject(row.getLong(CassandraDB.COLUMN_PRE));
			triple.setPredicate(row.getLong(CassandraDB.COLUMN_OBJ));
			tripleSet.add(triple);
		}
		for(Row row : resultSet3){
			Triple triple = null;
			triple.setSubject(row.getLong(CassandraDB.COLUMN_SUB));
			triple.setObject(row.getLong(CassandraDB.COLUMN_PRE));
			triple.setPredicate(row.getLong(CassandraDB.COLUMN_OBJ));
			tripleSet.add(triple);
		}
		return tripleSet;
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
	    	/*
	    	 * 先按只有一条要删除的算。
	    	 */

	    	
	    	//删除原始三元组 公理
	    	delOriginalTriple(session, sub, pre, obj);
	    	
	    	createIndexOnRSPO(session);    
	    	
	    	//删推理的三元组
			HashSet<Triple> tripleSet = new HashSet<Triple>();
			
			//可能需要重复sub pre obj
			tripleSet = getTriples(sub);
			
			//justification  可能需要不同的id作为表的key
			List<Long> delTripleList = new LinkedList<Long>();
			delTripleList.add(sub);
			delTripleList.add(pre);
			delTripleList.add(obj);

			Experiments.id = -201;  	//放到results表中。
			System.out.println("sub : " + sub + "  pre :  " + pre + "  obj : " + obj);
			String[] argStrings = {"--maptasks" , "8" , "--reducetasks" , "8" , "--subject" , sub.toString() , "--predicate" , pre.toString() , "--object" , obj.toString() ,"--clearoriginals"};
			System.out.println(argStrings);
			OWLHorstJustification.main(argStrings);
			Set<TupleValue> resultJustification = new HashSet<TupleValue>();
			
			CassandraDB db = null;
			Set<Set<TupleValue>> justifications = new HashSet<Set<TupleValue>>();
			Set<List<Long>> js = new HashSet<List<Long>>(); 	//justification
			try{
				db = new CassandraDB();
				db.getDBClient().set_keyspace(CassandraDB.KEYSPACE);
				//这个选择需要指定id  函数重载。
				justifications = db.getJustifications();	//得到justification
				for (Set<TupleValue> justification : justifications){
					System.out.println(">>>Justification - " + ":");
					for(TupleValue triple : justification){
						long sub1 = triple.getLong(0);
						long pre1 = triple.getLong(1);
						long obj1 = triple.getLong(2);
						System.out.println("\t<" + sub + ", " + pre + ", " + obj + ">" + 
								" - <" + db.idToLabel(sub) + ", " + db.idToLabel(pre) + ", " + db.idToLabel(obj) + ">");
						List<Long> list = new LinkedList<Long>();
						list.add(sub1);
						list.add(pre1);
						list.add(obj1);
						js.add(list);
					}
					//justification匹配，是否空
					if (js.contains(delTripleList)) {
						js.remove(delTripleList);
						//同时删除表中该行
						//用删除 或者 标记删除 ， 标记需要primary key 不止 partition key.
						String delCQL = " DELETE FROM justifications WHERE sub = + del0 AND  pre = 0 AND obj = 0";
						session.execute(delCQL);
						if (js.isEmpty() == true) {
							//添加该三元组循环
							
						}
					}
				}
			}catch(Exception e){
				System.err.println(e.getMessage());			
			}
			
			
			db.CassandraDBClose();
				
			
			
	    	
	    	
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
