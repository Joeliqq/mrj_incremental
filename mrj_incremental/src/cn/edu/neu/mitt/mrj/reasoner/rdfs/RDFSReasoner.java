package cn.edu.neu.mitt.mrj.reasoner.rdfs;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.reasoner.MapReduceReasonerJobConfig;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;
//import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class RDFSReasoner extends Configured implements Tool {
	
	protected static Logger log = LoggerFactory.getLogger(RDFSReasoner.class);
	private int numMapTasks = -1;
	private int numReduceTasks = -1;
	public static int step = 0;
	private int lastExecutionPropInheritance = -1;
	private int lastExecutionDomRange = -1; 
	

	private void parseArgs(String[] args) {
		
		for(int i=0;i<args.length; ++i) {
			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}
			
			// We will not support the following arguments for simplicity
			// That means we will not support stop and restart from breakpoints
//			if (args[i].equalsIgnoreCase("--startingStep")) {
//				RDFSReasoner.step = Integer.valueOf(args[++i]);
//			}
//
//			if (args[i].equalsIgnoreCase("--lastStepPropInher")) {
//				lastExecutionPropInheritance = Integer.valueOf(args[++i]);
//			}
//			
//			if (args[i].equalsIgnoreCase("--lastStepDomRange")) {
//				lastExecutionDomRange = Integer.valueOf(args[++i]);
//			}
		}
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("USAGE: RFDSReasoner [pool path] [options]");
			return;
		}
		
		try {
			ToolRunner.run(new Configuration(), new RDFSReasoner(), args);
		} catch (Exception e) {}
	}
	
	
	// The derivation will be launched in run()
	public long launchDerivation(String[] args) throws IOException, InterruptedException, ClassNotFoundException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {

		long time = System.currentTimeMillis();
		parseArgs(args);
		Job job = null;
		long derivation = 0;
		
		
		// RDFS subproperty inheritance reasoning
//		job = createNewJob("RDFS subproperty inheritance reasoning", "FILTER_ONLY_HIDDEN");
		job = MapReduceReasonerJobConfig.createNewJob(
				RDFSReasoner.class, 
				"RDFS subproperty inheritance reasoning", 
				new HashSet<Integer>(), 
				new HashSet<Integer>(), 		// Added by WuGang, 2015-07-13
				step,							// not used here
				numMapTasks, 
				numReduceTasks, true, true, 1);

		job.setMapperClass(RDFSSubPropInheritMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(RDFSSubpropInheritReducer.class);
		job.getConfiguration().setInt("reasoner.step", ++step);
		job.getConfiguration().setInt("lastExecution.step", lastExecutionPropInheritance);
		lastExecutionPropInheritance = step;
//TODO:		configureOutputJob(job, args[0], "dir-rdfs-derivation/dir-subprop-inherit");
			
		job.waitForCompletion(true);
		long propInheritanceDerivation = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		derivation += propInheritanceDerivation;

	
		
		// RDFS subproperty domain and range reasoning
//		job = createNewJob("RDFS subproperty domain and range reasoning", "FILTER_ONLY_HIDDEN");
		job = MapReduceReasonerJobConfig.createNewJob(
				RDFSReasoner.class,
				"RDFS subproperty domain and range reasoning", 
				new HashSet<Integer>(),
				new HashSet<Integer>(), 		// Added by WuGang, 2015-07-13
				step,							// not used here
				numMapTasks,
				numReduceTasks, true, true, 2);
		job.setMapperClass(RDFSSubPropDomRangeMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);	// Modified by WuGang, 2010-08-26
		job.setMapOutputValueClass(LongWritable.class);
		//job.setPartitionerClass(MyHashPartitioner.class);	// Is this ok? seems not necessary
		job.setReducerClass(RDFSSubpropDomRangeReducer.class);
		job.getConfiguration().setInt("reasoner.step", ++step);

		job.getConfiguration().setInt("lastExecution.step", lastExecutionDomRange);
		lastExecutionDomRange = step;
//TODO:		configureOutputJob(job, args[0], "dir-rdfs-derivation/dir-subprop-domain-range");
		job.waitForCompletion(true);
		long domRangeDerivation = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		derivation += domRangeDerivation;
		
		// RDFS cleaning up subprop duplicates
		// We remove it for simplicity. That means we will not support stop and restart from breakpoints 
		


	    //RDFS subclass reasoning
//		job = createNewJob("RDFS subclass reasoning", "FILTER_ONLY_TYPE_SUBCLASS");
		Set<Integer> filters = new HashSet<Integer>();
		filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBCLASS);
		job = MapReduceReasonerJobConfig.createNewJob(
				RDFSReasoner.class,
				"RDFS subclass reasoning", 
				filters,
				new HashSet<Integer>(), 		// Added by WuGang, 2015-07-13
				step,							// not used here
				numMapTasks,
				numReduceTasks, true, true, 3);
		job.setMapperClass(RDFSSubclasMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(RDFSSubclasReducer.class);
		job.getConfiguration().setInt("reasoner.step", ++step);

//		configureOutputJob(job, args[0], "dir-rdfs-output/dir-subclass-" + step);
		job.waitForCompletion(true);
		derivation += job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		
	    //calculate whether I derive some special properties
	    long specialTriples = 0;
	    Counter counter = job.getCounters().findCounter("RDFS derived triples", "subclass of resource");
	    specialTriples += counter.getValue();
	    counter = job.getCounters().findCounter("RDFS derived triples", "subclass of Literal");
	    specialTriples += counter.getValue();
	    counter = job.getCounters().findCounter("RDFS derived triples", "subproperty of member");
	    specialTriples += counter.getValue();
	    
		//Apply special derivations
		if (specialTriples > 0) {
			filters.clear();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBCLASS);
			filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBPROPERTY);
			filters.add(TriplesUtils.DATA_TRIPLE);
//		    job = createNewJob("RDFS special properties reasoning", "FILTER_ONLY_OTHER_SUBCLASS_SUBPROP");
			job = MapReduceReasonerJobConfig.createNewJob(
					RDFSReasoner.class,
					"RDFS special properties reasoning", 
					filters,
					new HashSet<Integer>(), 		// Added by WuGang, 2015-07-13
					step,							// not used here
					numMapTasks,
					numReduceTasks, true, true, 4);
			job.setMapperClass(RDFSSpecialPropsMapper.class);
			job.setMapOutputKeyClass(BytesWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setReducerClass(RDFSSpecialPropsReducer.class);
			job.getConfiguration().setInt("reasoner.step", ++step);
		
//			configureOutputJob(job, args[0], "dir-rdfs-output/dir-special-props-" + step);
		    job.waitForCompletion(true);
		    derivation += job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS").getValue();
		}
		
		log.info("RDFS reasoning time: " + (System.currentTimeMillis() - time));
		log.info("RDFS derivation: " + derivation);
		return derivation;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		launchDerivation(args);
		return 0;
	}
}