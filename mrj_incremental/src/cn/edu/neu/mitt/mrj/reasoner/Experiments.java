package cn.edu.neu.mitt.mrj.reasoner;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import jdk.internal.dynalink.beans.StaticClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.omg.CORBA.PUBLIC_MEMBER;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.justification.OWLHorstJustification;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Cluster.Builder;
public class Experiments {

	public static int id;

	public static void main(String[] args){
		Builder builder = Cluster.builder();
		builder.addContactPoint(CassandraDB.DEFAULT_HOST);
		SocketOptions socketoptions = new SocketOptions().setKeepAlive(true).setConnectTimeoutMillis(5 * 10000).setReadTimeoutMillis(100000);
		builder.withSocketOptions(socketoptions);
		Cluster cluster = builder.build();	
		Metadata metadata = cluster.getMetadata();	
		Session session = cluster.connect();
		
//		Random r = new Random(System.currentTimeMillis()) ;
//		int random = 0;
//		if (r.nextBoolean()) {
//		    random = r.nextInt(101) ;
//		} else {
//		    random = -r.nextInt(101) ;
//		}
		for (id = 0; id < 10; id++) {
			long random = ThreadLocalRandom.current().nextLong(-9223372036854775808L, 9223372036854775807L);
//			long startTime = System.currentTimeMillis();
			ResultSet results = session.execute("SELECT sub ,pre ,obj  FROM mrjks.resultrows WHERE TOKEN(isliteral , rule , sub ) > " + random + " LIMIT 1;");
//			System.out.println(results);
			for (Row row : results){
				Configuration conf = new Configuration();
				try {
					FileSystem hdfs = FileSystem.get(conf);
					Path deledir= new Path("/justification");
			        boolean isDeleted=hdfs.delete(deledir,true);
				} catch (IOException e1) {
					e1.printStackTrace();
				}		
				
//				System.out.println("id : " + id);

				Long sub, pre, obj;
				sub = row.getLong("sub");
				pre = row.getLong("pre");
				obj = row.getLong("obj");
				System.out.println("sub : " + sub + "  pre :  " + pre + "  obj : " + obj);
				//不能加空格
				String[] argStrings = {"--maptasks" , "8" , "--reducetasks" , "8" , "--subject" , sub.toString() , "--predicate" , pre.toString() , "--object" , obj.toString() ,"--clearoriginals"};
//				OWLHorstJustification OWJ = new OWLHorstJustification();			
				System.out.println(argStrings);
				OWLHorstJustification.main(argStrings);
				
//				try {
//					OWJ.launchClosure(argStrings);
//				} catch (ClassNotFoundException | IOException
//						| InterruptedException e) {
//					System.out.println("launchClosure error");
//					e.printStackTrace();
//				}
			
		}
//			System.out.println("Time (in seconds): " + (System.currentTimeMillis() - startTime) / 1000);		
		}
		cluster.close();
	}
}
