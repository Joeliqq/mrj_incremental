package cn.edu.neu.mitt.mrj.io.dbs;

import org.apache.cassandra.transport.SimpleClient;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class CreateTables {
	private Cluster cluster;
	private Session session;
	
	public Session getSession(){
		return this.session;
	}
	
	public void connect(String node){
		cluster = Cluster.builder()
				.addContactPoint(node)
				.build();	
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for(Host host : metadata.getAllHosts()){
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s|n", 
					host.getDatacenter(), host.getAddress(), host.getRack());	
		}		
		session = cluster.connect();
	}
	
	//javaDriver21
	public void createSchema(Integer step){
		session.execute("CREATE TABLE IF NOT EXISTS " + CassandraDB.KEYSPACE + ".step"  + step + 
				" ( " + 
                "sub" + " bigint, " + 
                "pre" + " bigint, " +
                "obj" + " bigint, " +	
        		"rule int, " +
                "v1" + " bigint, " +
                "v2" + " bigint, " +
                "v3" + " bigint, " +
                "transitivelevel int" + 
                ", primary key((sub, pre, obj, rule) ,v1, v2, v3 )) WITH compaction = {'class': 'LeveledCompactionStrategy'}");
	}
	
	public void close(){
		session.close();
		cluster.close();
	}
	
	public static void main(String args[]){
		CreateTables client = new CreateTables();
		client.connect(CassandraDB.DEFAULT_HOST);
		for (int i = 1; i < 14; i++) {
			client.createSchema(i);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		client.close();
	}
}
