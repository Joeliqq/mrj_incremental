package cn.edu.neu.mitt.mrj.reasoner;


import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class ReasonedJustifications extends Configured implements Tool{
	public int run(String[] args) throws Exception{	
		
		Configuration conf = new Configuration();
	    
		Job job = new Job(conf);
		job.setJobName(" Test ");
		job.setJarByClass(ReasonedJustifications.class);
	    job.setNumReduceTasks(8);
	    
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);
        ConfigHelper.setInputPartitioner(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.partitioner);
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
        CqlConfigHelper.setInputCql(job.getConfiguration(), 
	        		"SELECT * FROM " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS + 
	        		" WHERE TOKEN(" + 
	        		CassandraDB.COLUMN_SUB + ", " + 
	        		CassandraDB.COLUMN_PRE + ", " + 
	        		CassandraDB.COLUMN_OBJ +  
	        		//CassandraDB.COLUMN_IS_LITERAL +
	        		") > ? AND TOKEN(" + 
	        		CassandraDB.COLUMN_SUB + ", " + 
	        		CassandraDB.COLUMN_PRE + ", " + 
	        		CassandraDB.COLUMN_OBJ + 
	        		//CassandraDB.COLUMN_IS_LITERAL + 
	        		") <= ? ALLOW FILTERING");
	        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), CassandraDB.CQL_PAGE_ROW_SIZE);
	        //Modified by LiYang  
	        ConfigHelper.setInputSplitSize(job.getConfiguration(), 10000000);
	        job.setInputFormatClass(CqlInputFormat.class);
	        job.setOutputKeyClass(Map.class);
	        job.setOutputValueClass(List.class);
	        job.setOutputFormatClass(CqlOutputFormat.class);
	        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);
	        ConfigHelper.setOutputPartitioner(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.partitioner);

	        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
	        String query = "UPDATE " + CassandraDB.KEYSPACE + "." + "resultrows" +
	        		" SET " + CassandraDB.COLUMN_INFERRED_STEPS + "=?, " + CassandraDB.COLUMN_TRANSITIVE_LEVELS + "=?";
	        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
	        
		job.setMapperClass(ReasonedJustificationsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(ReasonedJustificationsReducer.class);
		
	    
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ReasonedJustifications(), args);
		System.exit(res);
	}
	
}
