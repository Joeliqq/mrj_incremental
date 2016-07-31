package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.db.DBAppender;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.io.files.readers.FilesTriplesReader;
import cn.edu.neu.mitt.mrj.partitioners.MyHashPartitioner;
import cn.edu.neu.mitt.mrj.reasoner.MapReduceReasonerJobConfig;
import cn.edu.neu.mitt.mrj.utils.Cassandraconf;
import cn.edu.neu.mitt.mrj.utils.FileUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;


public class OWLReasoner extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(OWLReasoner.class);

	public static final String OWL_OUTPUT_DIR = "/dir-owl-output/";
	public static final String RDFS_OUTPUT_DIR = "/dir-rdfs-output/";
	public static final String OWL_PROP_INHERITANCE_TMP = "/dir-tmp-prop-inheritance/";
	public static final String OWL_PROP_INHERITANCE = "/dir-prop-inheritance/";
	public static final String OWL_TRANSITIVITY_BASE = OWL_PROP_INHERITANCE_TMP + "dir-transitivity-base/";
	public static final String OWL_TRANSITIVITY = "dir-transitivity/";	// Added by WuGang 2010-08-25���¼ӵ�Ŀ¼

	public static final String OWL_SYNONYMS_TABLE = "dir-table-synonyms/";
	public static final String OWL_SYNONYMS_TABLE_NEW = "_table_synonyms_new/";
	public static final String OWL_SAME_AS_INHERITANCE_TMP = "/dir-tmp-same-as-inheritance/";
	
	public static final String OWL_EQUIVALENCE_TMP = "/dir-tmp-equivalence/";
	public static final String OWL_EQ_SUBPROP_SUBCLASS = "/dir-tmp-eq-subclass-subprop/";
	public static final String OWL_ALL_VALUE_TMP = "/dir-tmp-all-some-values/";
	public static final String OWL_HAS_VALUE_TMP = "/dir-tmp-has-value/";

	public CassandraDB db;
	
	private int numMapTasks = -1;
	private int numReduceTasks = -1;
	private int sampling = 0;
	private int resourceThreshold = 0;
	private long sizeDictionary = 0;
	private boolean shouldInferTransitivity = true;

	static long previousSomeAllValuesCycleDerivation = 0;
	public static int step = 0;
	static int previousInferPropertiesDerivation = -1;
	static int previousTransitiveDerivation = -1;
	static int previousHasValueDerivation = -1;

	private void parseArgs(String[] args) {
		
		for(int i=0;i<args.length; ++i) {
			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--samplingPercentage")) {
				sampling = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--samplingThreshold")) {
				resourceThreshold = Integer.valueOf(args[++i]);
			}
			
			
			// In order to distinguish from RDFS reasoning.
			if (args[i].equalsIgnoreCase("--startingStep")) {
				OWLReasoner.step = Integer.valueOf(args[++i]);
			}
		}
	}
	
	public static void main(String[] args) {
		System.out.println("Note: we should keep samplingPercentage small \n"
				+ "because it will generate a set of common resources in memory for dealing with SameAs.");
		if (args.length < 1) {
			System.out.println("USAGE: OWLReasoner [pool path] [options]");
			return;
		}
		
		try {
			OWLReasoner owlreasoner = new OWLReasoner();
//			owlreasoner.db = new CassandraDB(cn.edu.neu.mitt.mrj.utils.Cassandraconf.host, 9160);
//			owlreasoner.db.init();
			
			ToolRunner.run(new Configuration(), owlreasoner, args);
			
//			owlreasoner.db.CassandraDBClose();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.exit(0);
	}

	public long launchClosure(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		parseArgs(args);
		long derivedTriples = 0;
		long totalTriple = 0;
		long startTime = System.currentTimeMillis();
		
		boolean firstCycle = true;
		int currentStep = 0;
		int lastDerivationStep = 0;		
		
		//Modified 2015/6/28
		try {
			db = new CassandraDB();
//			db.init();  // 这不要init() 否则会出现 TTransportException: java.net.SocketException: 断开的管道
			/*
			 * getRowCountAccordingInferredSteps 类似的函数中出错。
			 * 具体原因不确定，可能跟client使用有关。
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}

		
		do {
			if (!firstCycle && lastDerivationStep == (currentStep - 4))
				break;
			currentStep++;
			System.out.println(">>>>>>>>>>> Start new OWL Reasoner loop <<<<<<<<<<<");
			long propDerivation = inferPropertiesInheritance(args); 
			System.out.println("----------- End inferPropertiesInheritance");
			//Get Attention!
			System.out.println("----------- Start inferTransitivityStatements");
			derivedTriples = inferTransitivityStatements(args) + propDerivation;
			System.out.println("----------- End inferTransitivityStatements");
			if (derivedTriples > 0) 
				lastDerivationStep = currentStep;
			
			if (!firstCycle && lastDerivationStep == (currentStep - 4))
				break;
			currentStep++;
			long sameAsDerivation = inferSameAsStatements(args);
			System.out.println("----------- End inferSameAsStatements");
			derivedTriples += sameAsDerivation;
			if (sameAsDerivation > 0) 
				lastDerivationStep = currentStep;
			
			if (!firstCycle && lastDerivationStep == (currentStep - 4))
				break;
			currentStep++;
			long equivalenceDerivation = inferEquivalenceStatements(args); 
			System.out.println("-----------inferEquivalenceStatements");
			derivedTriples += equivalenceDerivation;
			if (equivalenceDerivation > 0) lastDerivationStep = currentStep;
			
			if (!firstCycle && lastDerivationStep == (currentStep - 4))
				break;
			currentStep++;
			long hasValueDerivation = inferHasValueStatements(args);
			System.out.println("-----------inferHasValueStatements����");
			derivedTriples += hasValueDerivation;
			if (hasValueDerivation > 0) lastDerivationStep = currentStep;
			
			if (!firstCycle && lastDerivationStep == (currentStep - 4))
				break;
			currentStep++;
			long someAllDerivation = inferSomeAndAllValuesStatements(args);
			System.out.println("-----------inferSomeAndAllValuesStatements����");
			derivedTriples += someAllDerivation;
			if (someAllDerivation > 0) lastDerivationStep = currentStep;
			
			totalTriple += derivedTriples;
			firstCycle = false;
		} while (derivedTriples > 0);
		
		log.info("Time (in seconds): " + (System.currentTimeMillis() - startTime)/1000);
		log.info("Number derived triples: " + totalTriple);
		
		return totalTriple;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		launchClosure(args);
		return 0;
	}
	
	/*
	 * Executes rules 1,2,3 and 8a,8b.
	 * It also creates a directory with the triples on which will be executed later rule 4
	 */
	private long inferPropertiesInheritance(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		step++;
		Job job = MapReduceReasonerJobConfig.createNewJob(
				OWLReasoner.class,
				"OWL reasoner: infer properties inherited statements (not recursive), step " + step, 
				new HashSet<Integer>(),		//		FileUtils.FILTER_ONLY_HIDDEN.getClass(),
				new HashSet<Integer>(),		// not supported
				step,							// not used here
				numMapTasks,
				numReduceTasks, true, true, 5);		
		job.getConfiguration().setInt("reasoner.step", step);
		job.getConfiguration().setInt("reasoner.previosTransitiveDerivation", previousTransitiveDerivation);
		job.getConfiguration().setInt("reasoner.previousDerivation", previousInferPropertiesDerivation);
		previousInferPropertiesDerivation = step;
				
		job.setMapperClass(OWLNotRecursiveMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(OWLNotRecursiveReducer.class);

		job.waitForCompletion(true);
		
		
		Counter derivedTriples = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS");
		//Remove the transitivity triples
		long totalDerivation = derivedTriples.getValue();
		Counter transitivity = job.getCounters().findCounter("OWL derived triples", "transitive property input");
		if (transitivity != null) {
			totalDerivation -= transitivity.getValue();
		}
		Counter newTransitivity = job.getCounters().findCounter("OWL derived triples","new transitivity triples");
		if (newTransitivity.getValue() > 0)
			shouldInferTransitivity = true;
		else
			shouldInferTransitivity = false;

		return totalDerivation;
	}

	
	/*
	 * Executes rule 4
	 */
	private long inferTransitivityStatements(String[] args) 
											throws IOException, InterruptedException, ClassNotFoundException {
		boolean derivedNewStatements = true;
//		System.out.println("��inferTransitivityStatements��ͷ��");
		
		// We'll not use filesystem but db.getTransitiveStatementsCount()
		long derivation = 0;
		int level = 0;
		
		//modified 2015/5/19
		long beforeInferCount = db.getRowCountAccordingTripleTypeWithLimitation(TriplesUtils.TRANSITIVE_TRIPLE, 1);
		
		while ((beforeInferCount > 0) && derivedNewStatements && shouldInferTransitivity) {
//			System.out.println("��ʼ��inferTransitivityStatements��whileѭ����Ѱ�ҡ�");
			level++;

			Set<Integer> levels = new HashSet<Integer>();
			levels.add(new Integer(level-1));
			if (level > 1)
				levels.add(new Integer(level-2));
			
			//Configure input. Take only the directories that are two levels below
			Job job = null;
			
			// for the first two level, we use the whole data in the database
			if (level <= 2)	
				job = MapReduceReasonerJobConfig.createNewJob(
						OWLReasoner.class,
						"OWL reasoner: transitivity rule. Level " + level, 
						new HashSet<Integer>(),		//		FileUtils.FILTER_ONLY_HIDDEN.getClass(),
						new HashSet<Integer>(),
						0,
						numMapTasks,
						numReduceTasks, true, true, 6);		
			// for the level more than two, we only consider the last two level derived data in the current step
			if (level > 2)
				job = MapReduceReasonerJobConfig.createNewJob(
						OWLReasoner.class,
						"OWL reasoner: transitivity rule. Level " + level, 
						new HashSet<Integer>(),		//		FileUtils.FILTER_ONLY_HIDDEN.getClass(),
						levels,
						step,
						numMapTasks,
						numReduceTasks, true, true ,7);	
			
			
			job.getConfiguration().setInt("reasoning.baseLevel", step);
			job.getConfiguration().setInt("reasoning.transitivityLevel", level);
		    job.getConfiguration().setInt("maptasks", Math.max(numMapTasks / 10, 1));
					
			job.setMapperClass(OWLTransitivityMapper.class);
			job.setMapOutputKeyClass(BytesWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);
			job.setReducerClass(OWLTransitivityReducer.class);
			
			job.waitForCompletion(true);
			long stepNotFilteredDerivation = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS").getValue();
			
			long stepDerivation = 0;
			if (stepNotFilteredDerivation > 0) {
				try {
					db.createIndexOnInferredSteps();
				} catch (InvalidRequestException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnavailableException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TimedOutException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SchemaDisagreementException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				stepDerivation = db.getRowCountAccordingInferredSteps(level);
			}
			derivation += stepDerivation;
			derivedNewStatements = stepDerivation > 0;
		}
		
		previousTransitiveDerivation = step;
		return derivation;
	}

	
	
	private long inferSameAsStatements(String[] args) {
		Job job = null;
//		String inputSynonymsDir = args[0] + "/" + OWL_SYNONYMS_TABLE;
		long derivedTriples = 0;
		try {
			boolean derivedSynonyms = true;
			int derivationStep = 1;
//			long previousStepDerived = 0; 	// Added by WuGang 2015-01-30
			
			while (derivedSynonyms) {
				if (db.getRowCountAccordingTripleType(TriplesUtils.DATA_TRIPLE_SAME_AS)==0)	// We need not to infer on SameAs
					return 0;

				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.DATA_TRIPLE_SAME_AS);
				job = MapReduceReasonerJobConfig.createNewJob(
						OWLReasoner.class,
						"OWL reasoner: build the synonyms table from same as triples - step " + derivationStep++, 
						filters,		//		FileUtils.FILTER_ONLY_HIDDEN.getClass(),
						new HashSet<Integer>(), 		// Added by WuGang, 2015-07-12
						step,							// not used here
						numMapTasks,
						numReduceTasks, true, true, 8);		
			    
				job.setMapperClass(OWLSameAsMapper.class);
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(BytesWritable.class);
				job.setReducerClass(OWLSameAsReducer.class);
		      			
				job.waitForCompletion(true);
				
//				System.out.println("In FilesOWLReasoner: " + job.getCounters().findCounter("synonyms", "replacements").getValue());
				Counter cDerivedSynonyms = job.getCounters().findCounter("synonyms","replacements");
				long currentStepDerived = cDerivedSynonyms.getValue();	// Added by WuGang 2015-01-30
				derivedSynonyms = currentStepDerived > 0;	// Added by WuGang 2015-07-12
//				derivedTriples += currentStepDerived;
//				derivedSynonyms = (currentStepDerived - previousStepDerived) > 0;	// Modified by WuGang 2015-01-30
				//derivedSynonyms = currentStepDerived > 0;				
//				previousStepDerived = currentStepDerived;	// Added by WuGang 2015-01-30
			}
			
			//Filter the table.
			
			//modified 2015/5/19
			long tableSize = db.getRowCountAccordingTripleType(TriplesUtils.SYNONYMS_TABLE);
			
//			System.out.println("tableSize Ϊ : " + tableSize);
//			System.out.println("sizeDictionary Ϊ : " + sizeDictionary);
//			System.out.println("derivedTriples Ϊ : " + derivedTriples);
			
			//modified 2015/5/19
			if (tableSize > sizeDictionary || derivedTriples > 0) {
			//for(int j =0 ;j <= 3 ; j++){
				//1) Calculate the URIs distribution and get the first 2M.
				job = MapReduceReasonerJobConfig.createNewJob(
						OWLReasoner.class,
						"OWL reasoner: sampling more common resources", 
						new HashSet<Integer>(),		//		FileUtils.FILTER_ONLY_HIDDEN.getClass(),
						new HashSet<Integer>(),		// Added by WuGang, 2015-07-12
						step,							// not used here
						numMapTasks,
						numReduceTasks, true, false, 9);		// input from cassandra, but output to hdfs
				job.getConfiguration().setInt("reasoner.samplingPercentage", sampling); //Sampling at 10%
				job.getConfiguration().setInt("reasoner.threshold", resourceThreshold); //Threshold resources

				/*
				 * output to hdfs
				 */
				
				job.setMapperClass(OWLSampleResourcesMapper.class);
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(LongWritable.class);
				job.setReducerClass(OWLSampleResourcesReducer.class);
			    
				// config output to hdfs
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(LongWritable.class);
			    Path commonResourcesPath = new Path(new Path(args[0]), "_commonResources");
			    SequenceFileOutputFormat.setOutputPath(job, commonResourcesPath);
			    job.setOutputFormatClass(SequenceFileOutputFormat.class);
			    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
	    
			    job.waitForCompletion(true);
				
			    
			    
				//2) Launch a job that split the triples				
				job = new Job();
				job.setJarByClass(OWLReasoner.class);
				job.getConfiguration().setInt("maptasks", numMapTasks);
			    job.setNumReduceTasks(numReduceTasks);
				
				job.setJobName("OWL reasoner: replace triples using the sameAs synonyms: deconstruct triples");				
				job.setInputFormatClass(FilesTriplesReader.class);
				FilesTriplesReader.setInputPathFilter(job, FileUtils.FILTER_ONLY_HIDDEN.getClass());
				FilesTriplesReader.addInputPath(job, new Path(args[0]));
				
				job.setMapperClass(OWLSameAsDeconstructMapper.class);
				job.setPartitionerClass(MyHashPartitioner.class);
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(BytesWritable.class);
				job.setReducerClass(OWLSameAsDeconstructReducer.class);
			    
			    Path tmpPath = new Path(new Path(args[0]), "deconstructTriples");
			    SequenceFileOutputFormat.setOutputPath(job, tmpPath);
			    job.setOutputKeyClass(LongWritable.class);
			    job.setOutputValueClass(BytesWritable.class);
			    job.setOutputFormatClass(SequenceFileOutputFormat.class);
			    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
				job.waitForCompletion(true);
				
				
				
				//3) Launch a job that reconstruct the triples
				job = MapReduceReasonerJobConfig.createNewJob(
						OWLReasoner.class,
						"OWL reasoner: replace triples using the sameAs synonyms: reconstruct triples", 
						new HashSet<Integer>(),		//		FileUtils.FILTER_ONLY_HIDDEN.getClass(),
						new HashSet<Integer>(),		// Added by WuGang, 2015-07-12
						step,							// not used here
						numMapTasks,
						numReduceTasks, false, true, 10);		// input from hdfs, but output to cassandra

				SequenceFileInputFormat.addInputPath(job, tmpPath);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				
				job.setMapperClass(OWLSameAsReconstructMapper.class);
				job.setMapOutputKeyClass(BytesWritable.class);
				job.setMapOutputValueClass(BytesWritable.class);
				job.setReducerClass(OWLSameAsReconstructReducer.class);
							
				job.waitForCompletion(true);
				
				FileSystem fs = FileSystem.get(job.getConfiguration());
				fs.delete(tmpPath, true);
				fs.delete(commonResourcesPath, true);
				
			
			
				//Remove all the others directories. Keep only the last one produced.
				if (fs.exists(new Path(args[0] + "/dir-input")))
					fs.rename(new Path(args[0] + "/dir-input"), new Path(args[0] + "/_dir-input"));

			}
			//modified 2015/5/19
			sizeDictionary = tableSize;
			
		} catch (Exception e) {
			log.error("Job execution has failed", e);
		}
		
		return derivedTriples;
	}


	
	/* Implements rules 12abc, 13abc */
	private long inferEquivalenceStatements(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		step++;
		
		Set<Integer> filters = new HashSet<Integer>();
//		filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBCLASS);
//		filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBPROPERTY);
//		filters.add(TriplesUtils.SCHEMA_TRIPLE_EQUIVALENT_CLASS);
//		filters.add(TriplesUtils.SCHEMA_TRIPLE_EQUIVALENT_PROPERTY);

		Job job = MapReduceReasonerJobConfig.createNewJob(
				OWLReasoner.class,
				"OWL reasoner: infer equivalence from subclass and subprop. step " + step, 
				filters,
				new HashSet<Integer>(),		// Added by WuGang, 20150712
				step,							// not used here
				numMapTasks,
				numReduceTasks, true, true, 11);		
		job.getConfiguration().setInt("maptasks", Math.max(job.getConfiguration().getInt("maptasks", 0) / 10, 1));
		job.getConfiguration().setInt("reasoner.step", step);

		job.setMapperClass(OWLEquivalenceSCSPMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(OWLEquivalenceSCSPReducer.class);		
       	
		job.waitForCompletion(true);
		return job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS").getValue();
	}

	/*
	 * Executes rules 15 and 16
	 */
	private long inferSomeAndAllValuesStatements(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		boolean derivedNewStatements = true;
		long totalDerivation = 0;
		int previousSomeAllValuesDerivation = -1;
		boolean firstCycle = true;
		
		// Added by Wugang 20150111
		//long countRule15 = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_15);	// see OWLAllSomeValuesReducer
		//long countRule16 = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_16);	// see OWLAllSomeValuesReducer
		
		while (derivedNewStatements) {
			step++;
			
			Job job = MapReduceReasonerJobConfig.createNewJob(
					OWLReasoner.class,
					"OWL reasoner: some and all values rule. step " + step, 
					new HashSet<Integer>(),
					new HashSet<Integer>(),
					step,							// not used here
					numMapTasks,
					numReduceTasks, true, true, 12);		
			job.getConfiguration().setInt("reasoner.step", step);
			job.getConfiguration().setInt("reasoner.previousDerivation", previousSomeAllValuesDerivation);
			previousSomeAllValuesDerivation = step;

			job.setMapperClass(OWLAllSomeValuesMapper.class);
			job.setMapOutputKeyClass(BytesWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);
			job.setReducerClass(OWLAllSomeValuesReducer.class);
	       	
			job.waitForCompletion(true);
			
			// Added by Wugang 20150111
		//	countRule15 = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_15) - countRule15;	// see OWLAllSomeValuesReducer
		//	countRule16 = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_16) - countRule16;	// see OWLAllSomeValuesReducer
		//	totalDerivation =  countRule15 +  countRule16;

			
			Counter derivedTriples = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS");
			long notFilteredDerivation = derivedTriples.getValue();
			long stepDerivation = 0;
			if (firstCycle)
				notFilteredDerivation -= previousSomeAllValuesCycleDerivation;
			if (notFilteredDerivation > 0) {
				previousSomeAllValuesCycleDerivation += notFilteredDerivation;
				//Modified by LiYang 2015/9/21
//				try {
//					db.createIndexOnInferredSteps();
//				} catch (TException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				try {
					db.createIndexOnInferredSteps();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				stepDerivation = db.getRowCountAccordingInferredSteps(step - 1);
				totalDerivation += stepDerivation;
				derivedNewStatements = stepDerivation > 0;
			} else {
				derivedNewStatements = false;
			}
			firstCycle = false;
		}
		
		// Added by Wugang 20150111
		//countRule15 = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_15) - countRule15;	// see OWLAllSomeValuesReducer
		//countRule16 = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_16) - countRule16;	// see OWLAllSomeValuesReducer
		//totalDerivation =  countRule15 +  countRule16;
		
		return totalDerivation;
	}
	
	/*
	 * Executes rule 14a,14b
	 */
	private long inferHasValueStatements(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		step++;
		
		// Added by Wugang 20150111
		//long countRule14a = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_14a);	// see OWLAllSomeValuesReducer
		//long countRule14b = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_14b);	// see OWLAllSomeValuesReducer

		
		Job job = MapReduceReasonerJobConfig.createNewJob(
				OWLReasoner.class,
				"OWL reasoner: hasValue rule. step " + step, 
				new HashSet<Integer>(),
				new HashSet<Integer>(),
				step,							// not used here
				numMapTasks,
				numReduceTasks, true, true, 13);		
		
		long schemaOnPropertySize = db.getRowCountAccordingTripleType(TriplesUtils.SCHEMA_TRIPLE_ON_PROPERTY);
		if (schemaOnPropertySize == 0)
			return 0;

		job.getConfiguration().setInt("reasoner.step", step);
		job.getConfiguration().setInt("reasoner.previousStep",previousHasValueDerivation);
		previousHasValueDerivation = step;		
		
		job.setMapperClass(OWLHasValueMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(OWLHasValueReducer.class);		

		job.waitForCompletion(true);
		
		// Get inferred count
		if (job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue() > 0) {
		//	countRule14a = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_14a) - countRule14a;	// see OWLAllSomeValuesReducer
		//	countRule14b = db.getRowCountAccordingRule((int)TriplesUtils.OWL_HORST_14b) - countRule14b;	// see OWLAllSomeValuesReducer
		//	return(countRule14a +  countRule14b);
			try {
				db.createIndexOnInferredSteps();
			} catch (InvalidRequestException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnavailableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimedOutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SchemaDisagreementException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long stepDerivation = db.getRowCountAccordingInferredSteps(step - 1);
			return stepDerivation;
		} else {
			return 0;
		}
	}
	
}