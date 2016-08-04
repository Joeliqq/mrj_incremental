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
		
	private static void delOriginalTriple(Session session, Long sub, Long pre, Long obj){
 	    String cqlStatement = "DELETE FROM " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS + " WHERE sub = " + sub + " AND pre = " + pre + " AND obj = " + obj;
 	    session.execute(cqlStatement);
	}
	
	private static void delReasonedTriples(Session session, Long sub, Long pre, Long obj){
		HashSet<List<Long>> reasonedtriples = new HashSet<List<Long>>();
		//可能需要重复sub pre obj，可以在方法内部添加
		reasonedtriples = getTriples(sub);
		List<Long> delTriple = new LinkedList<Long>();
		delTriple.add(sub);
		delTriple.add(pre);
		delTriple.add(obj);
		
		//每个三元组做justification
		for (List<Long> triple : reasonedtriples){			
	
			Set<Set<List<Long>>> justification = getJustifications(triple); //得到justification 可能需要不同的id作为表的key
			//判断该三元组的justification 是否包括 del T
			//justification 类型是 set<frozen<tuple<bigint, bigint, bigint>>> 对应 set<set<list<long>>>
			//判断 第二个set 是否包含删除的T， 第一个set是否空
			boolean del = containTriple(justification, delTriple);	
			if (del == true) {
				delReasonedTriples(session, triple.get(0), triple.get(1), triple.get(2));
			}	
		}
		
	}
		
	private static boolean containTriple(Set<Set<List<Long>>> justification, List<Long> delTriple){	
		
		for (Set<List<Long>> j : justification){	//j为单个justification
			if (j.contains(delTriple)) {
				//这个justification如何删，对应表中一行？？？删除该行？？、
				justification.remove(j);	//删掉整个justification,表中也需要删？？？表中应该如何删。或者justification的行删了，不需要了？需要从表中标记删除？
				deljustification(j, delTriple);
			}
		}
		if (justification.isEmpty() == true) {
			//添加该三元组循环
			//同时删除表中该行
			//用删除 或者 标记删除 ， 标记需要primary key 不止 partition key.  这样可能删不掉？
			String delCQL = " DELETE FROM justifications WHERE sub = "  + delTriple.get(0) + "AND  pre = " + delTriple.get(1) + " AND obj = " + delTriple.get(2);
			session.execute(delCQL);
			return true;
		}
		
		return false;
		

	}
	
	private static Set<Set<List<Long>>> getJustifications(List<Long> tripleList){
		System.out.println("sub : " + tripleList.get(0) + "  pre :  " + tripleList.get(1) + "  obj : " + tripleList.get(2));   //???
		String[] argStrings = {"--maptasks" , "8" , "--reducetasks" , "8" , "--subject" , tripleList.get(0).toString() , "--predicate" , tripleList.get(1).toString() , "--object" , tripleList.get(2).toString() ,"--clearoriginals"};
		System.out.println(argStrings);
		OWLHorstJustification.main(argStrings);
			
		Set<TupleValue> resultJustification = new HashSet<TupleValue>();	
		CassandraDB db = null;
		Set<Set<TupleValue>> justifications = new HashSet<Set<TupleValue>>();
		Set<Set<List<Long>>> js = new HashSet<Set<List<Long>>>(); 	//justification 第一个set是多个justification，第二个set是多个T
		try{
			db = new CassandraDB();
			db.getDBClient().set_keyspace(CassandraDB.KEYSPACE);
			//这个选择需要指定id  函数重载。
			justifications = db.getJustifications();	//得到justification
			for (Set<TupleValue> justification : justifications){
				System.out.println(">>>Justification - " + ":");
				Set<List<Long>> T = new HashSet<List<Long>>();
				for(TupleValue triple : justification){
					long sub1 = triple.getLong(0);
					long pre1 = triple.getLong(1);
					long obj1 = triple.getLong(2);
					System.out.println("\t<" + sub1 + ", " + pre1 + ", " + obj1 + ">" + 
							" - <" + db.idToLabel(sub1) + ", " + db.idToLabel(pre1) + ", " + db.idToLabel(obj1) + ">");
					List<Long> list = new LinkedList<Long>();
					list.add(sub1);
					list.add(pre1);
					list.add(obj1);
					T.add(list);
				}
				js.add(T);
			}
		}catch(Exception e){
			System.err.println(e.getMessage());			
		}	
		db.CassandraDBClose();
		return js;
	}
	
	public static void createIndexOnRSPO(Session session){
		session.execute("CREATE INDEX on justifications (v1)");
		session.execute("CREATE INDEX on justifications (v2)");
		session.execute("CREATE INDEX on justifications (v3)");
	}
	
	/*
	 * 得到所有可能包含List(Triple)的大集合，用于做justification展开
	 * r1  是sub obj 和 pre
	 * 返回所有可能调试信息包含删除三元组的蕴含(三元组)
	 */
	private static HashSet<List<Long>> getTriples(Long r1){
		ResultSet resultSet1 = session.execute("SELECT sub ,pre ,obj  FROM mrjks.resultrows WHERE v1 = " + r1);
		ResultSet resultSet2 = session.execute("SELECT sub ,pre ,obj  FROM mrjks.resultrows WHERE v2 = " + r1);
		ResultSet resultSet3 = session.execute("SELECT sub ,pre ,obj  FROM mrjks.resultrows WHERE v2 = " + r1);

		//
//		TupleType theType = TupleType.of(DataType.bigint(), DataType.bigint(), DataType.bigint());
		HashSet<List<Long>> tripleSet = new HashSet<List<Long>>();
		
		for(Row row : resultSet1){
//			Triple triple = null;
			List<Long> list1 = new ArrayList<Long>();
			list1.add(row.getLong(CassandraDB.COLUMN_SUB));
			list1.add(row.getLong(CassandraDB.COLUMN_PRE));
			list1.add(row.getLong(CassandraDB.COLUMN_OBJ));
//			triple.setSubject(row.getLong(CassandraDB.COLUMN_SUB));
//			triple.setObject(row.getLong(CassandraDB.COLUMN_PRE));
//			triple.setPredicate(row.getLong(CassandraDB.COLUMN_OBJ));
			tripleSet.add(list1);
		}
		for(Row row : resultSet2){
			List<Long> list1 = new ArrayList<Long>();
			list1.add(row.getLong(CassandraDB.COLUMN_SUB));
			list1.add(row.getLong(CassandraDB.COLUMN_PRE));
			list1.add(row.getLong(CassandraDB.COLUMN_OBJ));
			tripleSet.add(list1);
		}
		for(Row row : resultSet3){
			List<Long> list1 = new ArrayList<Long>();
			list1.add(row.getLong(CassandraDB.COLUMN_SUB));
			list1.add(row.getLong(CassandraDB.COLUMN_PRE));
			list1.add(row.getLong(CassandraDB.COLUMN_OBJ));
			tripleSet.add(list1);
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

			HashSet<List<Long>> reasonedtriples = new HashSet<List<Long>>();
			//可能需要重复sub pre obj
			reasonedtriples = getTriples(sub);

			//justification  

			Experiments.id = -201;  	//放到results表中。

			delReasonedTriples(session, sub, pre, obj);

	    }
	    

		
	}

}
