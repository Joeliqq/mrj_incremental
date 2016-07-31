package cn.edu.neu.mitt.mrj.importtriples;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.neu.mitt.mrj.utils.TriplesUtils;


public class ImportTriplesSampleMapper extends Mapper<Text, Text, Text, NullWritable> {

	private Text oText = new Text();
	private Random random = new Random();
	private int sampling = 0;
	private Map<String,Long> preloadedURIs = TriplesUtils.getInstance().getPreloadedURIs();
	
    protected void map(Text key, Text value, Context context) {
    	//System.out.println("��ImportTriplesSampleMapper��");
		try {
			String[] uris = TriplesUtils.parseTriple(value.toString(), key.toString());
			for(String uri : uris) {
				int randomNumber = random.nextInt(100);
				if (randomNumber < sampling && !preloadedURIs.containsKey(uri)) {
					oText.set(uri);
					context.write(oText, NullWritable.get());
				}
			}
		} catch (Exception e) {}
    }
    
    protected void setup(Context context) throws IOException, InterruptedException {
    	sampling = context.getConfiguration().getInt("reasoner.samplingPercentage", 0);
    }
}