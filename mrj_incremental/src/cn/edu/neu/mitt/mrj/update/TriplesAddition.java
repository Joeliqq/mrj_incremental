package cn.edu.neu.mitt.mrj.update;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.edu.neu.mitt.mrj.importtriples.FilesImportTriples;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSReasoner;

public class TriplesAddition {

	public static void main(String[] args){
		
		/*
		 * import
		 */
		Configuration conf = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(conf);
			Path deledir= new Path("/result");
	        boolean isDeleted=hdfs.delete(deledir,true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}		
		try {
			FilesImportTriples.main(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*
		 * reasoner
		 */
		CassandraDB.updatelabel = 1;
		/*
		 * args 需要更改
		 */
		RDFSReasoner.main(args);
		
	}
}
