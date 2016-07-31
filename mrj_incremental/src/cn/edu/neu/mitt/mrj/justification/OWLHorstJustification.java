/**
 * Project Name: mrj-0.1
 * File Name: OWLHorstJustification.java
 * @author Gang Wu
 * 2015��2��5�� ����4:58:08
 * 
 * Description: 
 * TODO
 */
package cn.edu.neu.mitt.mrj.justification;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Set;

import jdk.internal.dynalink.beans.StaticClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.omg.CORBA.PUBLIC_MEMBER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.Context;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.reasoner.Experiments;
import cn.edu.neu.mitt.mrj.utils.TripleKeyMapComparator;







//modified  cassandra java 2.0.5
import com.datastax.driver.core.TupleValue;

/**
 * This class is used to find justifications under OWL Horst semantics
 *
 */
public class OWLHorstJustification extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(OWLHorstJustification.class);

	// Parameters
	private int numMapTasks = -1;
	private int numReduceTasks = -1;
	public static long sub = -1;
	public static long pre = -1;
	public static long obj = -1;
	public static Path justificationsDirBase = new Path("/justification");
	
	public static long totaltriples;
	private static int tripleamount = 0;
	public static int id;	 //??

	private boolean bClearOriginals = false;

	/**
	 * 
	 */
	public OWLHorstJustification() {
	}

	/**
	 * @param conf
	 */
	public OWLHorstJustification(Configuration conf) {
		super(conf);
	}
	
	public void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equalsIgnoreCase("--subject")) 
				sub = Long.valueOf(args[++i]);
			if (args[i].equalsIgnoreCase("--predicate")) 
				pre = Long.valueOf(args[++i]);
			if (args[i].equalsIgnoreCase("--object")) 
				obj = Long.valueOf(args[++i]);
			if (args[i].equalsIgnoreCase("--maptasks")) 
				numMapTasks = Integer.valueOf(args[++i]);
			if (args[i].equalsIgnoreCase("--reducetasks")) 
				numReduceTasks = Integer.valueOf(args[++i]);

			// Added by WuGang 2015-06-08
			if (args[i].equalsIgnoreCase("--clearoriginals"))
				bClearOriginals = true;
		}
	}

	
	
	public static void prepareInput(long sub, long pre, long obj, boolean literal) {
		Triple tripleToJustify = new Triple(sub, pre, obj, literal);
		// An explanation is a set which is implemented here with a MapWritable taking NullWritable as value
		MapWritable initialExplanation = new MapWritable(); 
		initialExplanation.put(tripleToJustify, NullWritable.get());	 

		Configuration conf = new Configuration();
		try {
			int step = 0;
			Path justificationsDir = new Path(justificationsDirBase, String.valueOf(step)); // ��Ŀ¼�£����һ����original���ļ����ڴ洢��ʼ��justification��triple
			FileSystem fs = FileSystem.get(URI.create(justificationsDir.toString()), conf);
			if (!fs.exists(justificationsDir)) {
				SequenceFile.Writer writer = SequenceFile.createWriter(fs,
						conf, justificationsDir, Triple.class, MapWritable.class);
				writer.append(tripleToJustify, initialExplanation);
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	
	private Job createJustificationJob(int step) throws IOException {
		Path inputPath = new Path(justificationsDirBase, String.valueOf(step));
		Path outputPath =  new Path(justificationsDirBase, String.valueOf(step+1));

		// Job
		Configuration conf = new JobConf();
		conf.setInt("maptasks", numMapTasks);

		conf.setInt("id", id);
		
		Job job = new Job(conf);
		job.setJobName("OWL Horst Justification - Step " + step);
		job.setJarByClass(OWLHorstJustification.class);
		job.setNumReduceTasks(numReduceTasks);
		
	    //Input
	    FileInputFormat.addInputPath(job, inputPath);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    
	    //Mapper
	    job.setMapperClass(OWLHorstJustificationMapper.class);
	    job.setMapOutputKeyClass(MapWritable.class);			// map output an explanation as key
	    // map output a long value indicating whether an explanation need to be further expanded 
	    //     (if the value equals the size of the size of triples in the explanation)
	    job.setMapOutputValueClass(LongWritable.class);
	    
		((JobConf) job.getConfiguration()).setOutputKeyComparatorClass(TripleKeyMapComparator.class);

	    
	    //Reducer
	    job.setReducerClass(OWLHorstJustificationReducer.class);

	    //Output
	    SequenceFileOutputFormat.setOutputPath(job, outputPath);
	    job.setOutputKeyClass(Triple.class);					// reduce output key (in next loop it will be tried to expanded)
	    job.setOutputValueClass(MapWritable.class);				// reduce output value is an explanation
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
		

		return job;
	}
	

	public long launchClosure(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		parseArgs(args);
		
		// Added by WuGang 2015-06-08
//		if (bClearOriginals)
//			CassandraDB.removeOriginalTriples();

		
		long total = 0;			// Total justifications
		long newExpanded = -1;	// count of explanations that expanded in this loop
		long startTime = System.currentTimeMillis();
		int step = 0;
		
		id = Experiments.id + 200;
		System.out.println("id : " + id);


		prepareInput(sub, pre, obj, false);	// Default it is not a literal.
		
		File outputFile = new File("output");
		outputFile.createNewFile();
        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile, true));  
        
//      out.write("id : " + id + "\r\n");
//		System.out.println(sub + "       " + pre + "         " + obj);       
        out.write("id : " + id + "\r\n");
        out.write("sub : " + sub + "  pre :  " + pre + " obj :  " + obj + "\r\n");

        
		// find justifications
		do{
			log.info(">>>>>>>>>>>>>>>>>>>> Processing justification in step - " + step + " <<<<<<<<<<<<<<<<<<<<<<<<<");
	        
			out.write("step : " + step + "\r\n");

//			out.write("total : " + totaltriples + "\r\n");
						
			Job job = createJustificationJob(step);

			job.waitForCompletion(true);
			
//			int Retotal = 0;
//			Retotal = conf.getInt("id", 111);		
			//需要在 job.waitForCompletion(true); 之后。
	        Long result = job.getCounters().findCounter("Triples", "Triples").getValue();
			out.write("Reduce triples : " + result + "\r\n");

		
			newExpanded = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();

			Counter counterToProcess = job.getCounters().findCounter("OWL Horst Justifications Job", "ExplanationOutputs");
			total += counterToProcess.getValue();
			

			
			step++;
		}while (newExpanded > 0);
		//modified  cassandra java 2.0.5
		
		CassandraDB db = null;

		try{
			db = new CassandraDB();
			db.getDBClient().set_keyspace(CassandraDB.KEYSPACE);
			Set<Set<TupleValue>> justifications = db.getJustifications();
			int count = 0; 

			for (Set<TupleValue> justification : justifications){
//				int tripleamount = 0;
//				System.out.println(">>>Justification - " + ++count + ":");
//		        out.write(">>>Justification - " + ++count + ":" + "\r\n");
				for(TupleValue triple : justification){
					long sub = triple.getLong(0);
					long pre = triple.getLong(1);
					long obj = triple.getLong(2);
//					System.out.println("\t<" + sub + ", " + pre + ", " + obj + ">" + 
//							" - <" + db.idToLabel(sub) + ", " + db.idToLabel(pre) + ", " + db.idToLabel(obj) + ">");
//			        out.write("\t<" + sub + ", " + pre + ", " + obj + ">" + 
//							" - <" + db.idToLabel(sub) + ", " + db.idToLabel(pre) + ", " + db.idToLabel(obj) + ">" + "\r\n");
					tripleamount++;
				}
//				System.out.println(tripleamount);
		        out.write("tripleamount : " + tripleamount + "\r\n");
			}

			db.CassandraDBClose();
			
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
			
		System.out.println("Time (in seconds): " + (System.currentTimeMillis() - startTime) / 1000);
		System.out.println("Number justifications: " + total);

//		out.write("tripleamount : " + tripleamount + "\r\n");

        out.write("Time (in seconds): " + (System.currentTimeMillis() - startTime) / 1000 + "\r\n");
        out.write("Number justifications: " + total + "\r\n\r\n");
        out.flush();
        out.close(); 
		
		
		return total;

	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		launchClosure(args);
		return 0;
	}

	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("USAGE: OWLHorstJustification [options]");
			return;
		}

		try {
			ToolRunner.run(new Configuration(), new OWLHorstJustification(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
