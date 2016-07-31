/**
 * 
 */
package cn.edu.neu.mitt.mrj.io.dbs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author L
 *
 */

public class MrjMultioutput<KEYOUT, VALUEOUT> extends MultipleOutputs<KEYOUT, VALUEOUT> {

	private Map<String, TaskAttemptContext> taskContexts = new HashMap<String, TaskAttemptContext>();
	
	public MrjMultioutput(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
		super(context);
	}
	
	

	//This is copied from hadoop 0.23.11
	// maybe resolve the problem of construct job redundantly
	@Override
	protected TaskAttemptContext getContext(String nameOutput)
			throws IOException {
		TaskAttemptContext taskContext = taskContexts.get(nameOutput);
		
		if (taskContext != null) {
			return taskContext;
		}
		
		// The following trick leverages the instantiation of a record writer via
		// the job thus supporting arbitrary output formats.
		Job job = new Job(context.getConfiguration());
		job.setOutputFormatClass(getNamedOutputFormatClass(context, nameOutput));
		job.setOutputKeyClass(getNamedOutputKeyClass(context, nameOutput));
		job.setOutputValueClass(getNamedOutputValueClass(context, nameOutput));
		
	    taskContext = new TaskAttemptContext(
	    	      job.getConfiguration(), context.getTaskAttemptID());
		
		taskContexts.put(nameOutput, taskContext);
		
		return taskContext;
	}



	@Override
	protected synchronized RecordWriter getRecordWriter(
			TaskAttemptContext taskContext, String columnFamilyName)
			throws IOException, InterruptedException {
		
		
	    // look for record-writer in the cache
	    RecordWriter writer = recordWriters.get(columnFamilyName);
	    
//		System.out.println("get Record Writer");
	    
	    // If not in cache, create a new one
	    if (writer == null) {
	      // get the record writer from context output format
//	      FileOutputFormat.setOutputName(taskContext, baseFileName);
//			System.out.println("Before ConfigHelper.setOutputColumnFamily");
//			System.out.println(ConfigHelper.getOutputColumnFamily(taskContext.getConfiguration()));
	    	
	    	
			ConfigHelper.setOutputColumnFamily(taskContext.getConfiguration(), columnFamilyName);
//			CqlConfigHelper.setOutputCql(taskContext.getConfiguration(), getCql(columnFamilyNameName));

//			CqlBulkOutputFormat.setColumnFamilySchema(
//					taskContext.getConfiguration(), 
//					columnFamilyName, 
//					getSchema(columnFamilyName));
//					
//			CqlBulkOutputFormat.setColumnFamilyInsertStatement(
//					taskContext.getConfiguration(), 
//					columnFamilyName, 
//					getInsertStatement(columnFamilyName));
			

			
	      try {
//	    	  System.out.println(taskContext.getOutputFormatClass());
	        writer = ((OutputFormat) ReflectionUtils.newInstance(
	          taskContext.getOutputFormatClass(), taskContext.getConfiguration()))
	          .getRecordWriter(taskContext);
	        
//	        System.out.println(writer.getClass());
	      } catch (ClassNotFoundException e) {
	        throw new IOException(e);
	      }
	 
	      // if counters are enabled, wrap the writer with context 
	      // to increment counters 
	      if (countersEnabled) {
	        writer = new MultipleOutputs.RecordWriterWithCounter(writer, columnFamilyName, context);
	      }
	      
	      // add the record-writer to the cache
	      recordWriters.put(columnFamilyName, writer);
	    }
	    return writer;
	}

	
	String getCql(String columnFamilyNameName){
		if (columnFamilyNameName == "alltriples") {
			System.out.println("get cql allt");
			return ("UPDATE alltriples SET inferredsteps =? , isliteral =? , tripletype =?");
		}
		System.out.println("get cql step");
		return("UPDATE " + columnFamilyNameName + " SET transitivelevel =? ");
	}
	
//	String getSchema(String columnFamilyNameName){
////		System.out.println(columnFamilyNameName + " schema");
//		if (columnFamilyNameName == "alltriples") {
//			return CassandraDB.getAlltripleSchema();
//		}
//		return CassandraDB.getStepsSchema(columnFamilyNameName);
//	}
//	
//	String getInsertStatement(String columnFamilyNameName){
////		System.out.println(columnFamilyNameName + " insert statement");
//		if (columnFamilyNameName == "alltriples") {
//			return CassandraDB.getAlltripleStatement();
//		}
//		return CassandraDB.getStepsStatement(columnFamilyNameName);
//	}
	
}
