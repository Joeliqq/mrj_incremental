/**
 * 
 */
package cn.edu.neu.mitt.mrj.justification;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.io.dbs.SimpleClientDataStax;
import cn.edu.neu.mitt.mrj.reasoner.Experiments;

import com.datastax.driver.core.DataType;
//modified  cassandra java 2.0.5
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;

/**
 * @author gibeon
 *
 */
public class OWLHorstJustificationReducer extends
		Reducer<MapWritable, LongWritable, Triple, MapWritable> {

//	private static Logger log = LoggerFactory.getLogger(OWLHorstJustificationReducer.class);
	private static SimpleClientDataStax sClient = null;

	private long triplenum = 0;

	@Override
	protected void reduce(MapWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long total = 0; 
		int id = 0;
		Configuration reduceconf = context.getConfiguration();
		id = reduceconf.getInt("id", 2);
		
		for (LongWritable count:values){
			total += count.get();
		}
		triplenum = total;
		System.out.println("Reduce total count is: " + total);
		//modified  cassandra java 2.0.5
		
//		reduceconf.setInt("id", (int)total);
		
		
		
		
//		File outputFile = new File("output");
//		outputFile.createNewFile();
//        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile, true));  
//        out.write("Total count is: " + total);
//        out.flush();
//        out.close(); 

//        try{
//            Path pt=new Path("./result");
//            FileSystem fs = FileSystem.get(new Configuration());
//            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
//                                       // TO append data to a file, use fs.append(Path f)
//            String line;
//            line="Total count is: " + total;
//            System.out.println(line);
//            br.write(line);
//            br.close();
//        }catch(Exception e){
//            System.out.println("File not found");
//        }
		
//        System.out.println("Reduce id : " + Experiments.id);	//均是0
		
		
		if (total == key.size()){	// Find a candidate justification, output it to the database
			Set<TupleValue> resultJustification = new HashSet<TupleValue>();
			for(Writable triple : key.keySet()){
				TupleType theType = TupleType.of(DataType.bigint(), DataType.bigint(), DataType.bigint());
				TupleValue theValue = theType.newValue();
				theValue.setLong(0, ((Triple)triple).getSubject());
				theValue.setLong(1, ((Triple)triple).getPredicate());
				theValue.setLong(2, ((Triple)triple).getObject());
				resultJustification.add(theValue);
				System.out.println(" _______ " + ((Triple)triple).getSubject());
			}
			
			System.out.println("Write a candidate justification to database=========== ");
			System.out.println(resultJustification.toString());
//			log.info("Write a candidate justification to database=========== ");
//			log.info(resultJustification.toString());
			
			System.out.println(" REDUCE id : " + id);
			Insert insert = QueryBuilder
					.insertInto(CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_RESULTS)
					.value(CassandraDB.COLUMN_JUSTIFICATION, resultJustification)
					.value("id", id);
			sClient.getSession().execute(insert);
			
			// Added by WuGang 2015-02-14
			context.getCounter("OWL Horst Justifications Job", "ExplanationOutputs").increment(1);
		}else if (total == 0){		// Should be further traced
			for (Writable triple : key.keySet()){
//				System.out.println("find a triple to be further traced: " + (Triple)triple);
				context.write((Triple)triple, key);
			}
		}	// else do nothing.
		
//		OWLHorstJustification.totaltriples = total;

	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		sClient = new SimpleClientDataStax();
		sClient.connect(CassandraDB.DEFAULT_HOST);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		context.getCounter("Triples", "Triples").increment(triplenum);
		
		sClient.close();
	}
}
