/**
 * Project Name: mrj-0.1
 * File Name: SimpleClientDataStax.java
 * @author Gang Wu
 * 2015年2月7日 下午10:31:10
 * 
 * Description: 
 * TODO
 */
package cn.edu.neu.mitt.mrj.io.dbs;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * @author gibeo_000
 *
 */
public class SimpleClientDataStax {

	private Cluster cluster;
	private Session session;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}
	
	public Session getSession(){
		return session;
	}

	public void close() {
		cluster.close();
	}

	public static void main(String[] args) {
		SimpleClientDataStax client = new SimpleClientDataStax();
		client.connect("127.0.0.1");
		client.close();
	}

}
