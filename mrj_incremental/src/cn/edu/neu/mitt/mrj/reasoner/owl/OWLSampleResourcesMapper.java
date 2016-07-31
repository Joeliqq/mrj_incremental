package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

public class OWLSampleResourcesMapper extends Mapper<Long, Row, LongWritable, LongWritable> {
	
	private LongWritable oKey = new LongWritable();
	private LongWritable oValue = new LongWritable();
	private Random random = new Random();
	private int threshold = 0;
	
	@Override
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

		if (value.getPredicate() == TriplesUtils.OWL_SAME_AS) {
			if (value.getSubject() != value.getObject()) {
				oKey.set(value.getObject());
				oValue.set(value.getSubject());
				context.write(oKey, oValue);
			}
		} else {
			//Output only 10% of the dataset
			int randomNumber = random.nextInt(100);
			if (randomNumber < threshold) {
				oValue.set(0);
				oKey.set(value.getSubject());
				context.write(oKey, oValue);
				oKey.set(value.getPredicate());
				context.write(oKey, oValue);
				oKey.set(value.getObject());
				context.write(oKey, oValue);
			}
		}
	}
	
	public void setup(Context context) {

		threshold = context.getConfiguration().getInt("reasoner.samplingPercentage", 0);
	}
}