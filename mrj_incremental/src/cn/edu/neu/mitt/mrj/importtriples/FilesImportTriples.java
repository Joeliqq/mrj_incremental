package cn.edu.neu.mitt.mrj.importtriples;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.io.files.readers.NTriplesReader;
import cn.edu.neu.mitt.mrj.io.files.writers.FilesCombinedWriter;
import cn.edu.neu.mitt.mrj.io.files.writers.FilesDictWriter;


public class FilesImportTriples extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(FilesImportTriples.class);
	
	//Parameters
	private int numReduceTasks = 1;
	private int numMapTasks = 1;
	private int sampling = 0;
	private int resourceThreshold = 0;
	
//	private static final String CASSANDRA_PRIMARY_KEY = "row_key";	// key in hadoop job config context

	/**
	 * Step 1: sampleCommonResources -- sample those common resources
	 * Step 2: assignIdsToNodes -- assign ids to nodes
	 * Step 3: rewriteTriples -- use the node ids to update triples
	 */
	public int run(String[] args) throws Exception {
		parseArgs(args);
		FileSystem fs = FileSystem.get(this.getConf());
		fs.delete(new Path(args[1]), true);
		sampleCommonResources(args);
		assignIdsToNodes(args);
		fs.rename(new Path(args[1],"_dict/others"), new Path(args[2],"_dict/others"));
		rewriteTriples(args);
		fs.delete(new Path(args[1]), true);
		return 0;
	}

	private Job createNewJob(String name) throws IOException {
		Configuration conf = new Configuration();
		conf.setInt("reasoner.samplingPercentage", sampling);
	    conf.setInt("reasoner.threshold", resourceThreshold);
	    conf.setInt("maptasks", numMapTasks);
		
		Job job = new Job(conf);
		job.setJarByClass(FilesImportTriples.class);
		job.setJobName(name);
		FileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
		job.setNumReduceTasks(numReduceTasks);
		SequenceFileOutputFormat.setCompressOutput(job, true);
	    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		return job;
	}
	
	public void parseArgs(String[] args) {
		
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
		}
	}
	
	public void sampleCommonResources(String[] args) throws Exception {
//		System.out.println("��sampleCommonResources�����С�");
		Job job = createNewJob("Sample common resources");
		
	    //Input
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    job.setInputFormatClass(NTriplesReader.class);
	    
	    //Job
	    job.setMapperClass(ImportTriplesSampleMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    job.setReducerClass(ImportTriplesSampleReducer.class);

	    //Output
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(BytesWritable.class);
	    FilesDictWriter.writeCommonResources(job.getConfiguration(), true);
	    SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]));
	    job.setOutputFormatClass(FilesDictWriter.class);
	    
	    long time = System.currentTimeMillis();
	    job.waitForCompletion(true);
	    log.info("Job finished in " + (System.currentTimeMillis() - time));
	}
	
	public void assignIdsToNodes(String[] args) throws Exception {
		
		Job job = createNewJob("Deconstruct statements");
		job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
		job.getConfiguration().set("commonResources", args[2] + "/_dict/commons");
		
	    //Input
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    job.setInputFormatClass(NTriplesReader.class);
	    
	    //Job
	    job.setMapperClass(ImportTriplesDeconstructMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setReducerClass(ImportTriplesDeconstructReducer.class);

	    //Output
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(BytesWritable.class);
	    job.setOutputFormatClass(FilesCombinedWriter.class);
	    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

	    //Launch
	    long time = System.currentTimeMillis();
	    job.waitForCompletion(true);
	    log.info("Job finished in " + (System.currentTimeMillis() - time));
	}

	private void rewriteTriples(String[] args) throws Exception {		
		Job job = createNewJob("Reconstruct statements");

	    //Input
	    SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
	    job.setInputFormatClass(SequenceFileInputFormat.class);

	    //Job
	    job.setMapperClass(ImportTriplesReconstructMapper.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    //Output
	    Path outputDir = new Path(args[2], "dir-input");
  
	    
	    //New job settings
	    job.setReducerClass(ImportTriplesReconstructReducerToCassandra.class);
        job.setOutputKeyClass(Map.class);
        job.setOutputValueClass(List.class);
        job.setOutputFormatClass(CqlOutputFormat.class);

        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
       
        String query = "UPDATE " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS +
	        	" SET " + CassandraDB.COLUMN_TRANSITIVE_LEVELS + "= null ,"+ CassandraDB.COLUMN_INFERRED_STEPS + " = 0 , " + CassandraDB.COLUMN_UPDATELABEL + " = 0 " ;			 
		
        System.out.println("args.length : " + args.length);
        
        if (args.length > 11) {
	        System.out.println("args[11] : " + args[11]);
	        //先 只增加一次
	        if (args[11].equalsIgnoreCase("1")){
				query = "UPDATE " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS +
						" SET " + CassandraDB.COLUMN_TRANSITIVE_LEVELS + "= null ,"+ CassandraDB.COLUMN_INFERRED_STEPS + " = 0 , " + CassandraDB.COLUMN_UPDATELABEL + " = " + args[11] ;		 
		        System.out.println(query);
	        }
		}

        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
       
        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.partitioner);
	    
	    //Launch
	    long time = System.currentTimeMillis();
	    job.waitForCompletion(true);
	    log.info("Job finished in " + (System.currentTimeMillis() - time));
	    
		//Check if there is synonyms table I move it to the root folder
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path synTableDir = new Path(outputDir, "dir-synonymstable");
		if (fs.exists(synTableDir)) {
			Path parent = new Path(args[2], "dir-table-synonyms/");
			if (!fs.exists(parent)) {
				fs.mkdirs(parent);
			}
			Path newSynTableDir = new Path(parent, "dir-input");
			fs.rename(synTableDir, newSynTableDir);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: ImportTriples [input dir] [tmp dir] [output dir]");
			System.exit(0);
		}

		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new FilesImportTriples(), args);
	
		System.out.println("Import time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	  }	
}