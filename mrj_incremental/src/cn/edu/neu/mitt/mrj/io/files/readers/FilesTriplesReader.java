package cn.edu.neu.mitt.mrj.io.files.readers;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;

public class FilesTriplesReader extends MultiFilesReader<TripleSource, Triple> {
	
	protected static Logger log = LoggerFactory.getLogger(FilesTriplesReader.class);

	@Override
	public RecordReader<TripleSource, Triple> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FilesTriplesRecordReader();
	}

	public static boolean loadSetIntoMemory(Set<Long> schemaTriples, JobContext context, String filter, int previousStep) throws IOException {
		return loadSetIntoMemory(schemaTriples, context, filter, previousStep, false);
	}
	
	public static boolean loadSetIntoMemory(Set<Long> schemaTriples, JobContext context, String filter, int previousStep, boolean inverted)
											throws IOException {
		long startTime = System.currentTimeMillis();
		boolean schemaChanged = false;

		TripleSource key = new TripleSource();
		Triple value = new Triple();

		List<FileStatus> files = recursiveListStatus(context, filter); 
		log.info("In FilesTriplesReader's loadSetIntoMemory, there are " + files.size() + " files to be processed."); // Added by WuGang
		for(FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
				log.info("In FilesTriplesReader's loadSetIntoMemory, processing: " + file.getPath());	// Added by WuGang
				input = new SequenceFile.Reader(fs, file.getPath(), context.getConfiguration());
				boolean nextTriple = false;
				do {
					nextTriple = input.next(key, value);
					log.info("Loading triples into memory as set: "+value);	// Added by WuGang
					if (nextTriple) {
						if (!inverted)
							schemaTriples.add(value.getSubject());
						else
							schemaTriples.add(value.getObject());
						if (key.getStep() > previousStep) {
							schemaChanged = true;
						}
					}
				} while (nextTriple); 
				
			} finally {	
				if (input != null) {		
					input.close();
				}
			}
		}
		
//		log.info("loadSetIntoMemory's schemaTriples: " + schemaTriples);	// Added by WuGang

		log.debug("Time for loading the schema files is " + (System.currentTimeMillis() - startTime));
		return schemaChanged;
	}
	
	public static Map<Long, Collection<Long>> loadMapIntoMemory(String filter, JobContext context) throws IOException {
		return loadMapIntoMemory(filter, context, false);
	}
	
	// 返回的key就是triple的subject，value是object
	public static Map<Long, Collection<Long>> loadMapIntoMemory(String filter, JobContext context, boolean inverted) throws IOException {
		Map<Long, Collection<Long>> schemaTriples = new HashMap<Long, Collection<Long>>();
		TripleSource key = new TripleSource();
		Triple value = new Triple();

		List<FileStatus> files = recursiveListStatus(context, filter);
		for(FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
//				log.info("In FilesTriplesReader's loadMapIntoMemory, processing: " + file.getPath());	// Added by WuGang
				input = new SequenceFile.Reader(fs, file.getPath(), context.getConfiguration());
				boolean nextTriple = false;
				do {
					nextTriple = input.next(key, value);
					
//					log.info("Loading triples into memory as map: "+value);	// Added by WuGang
										
					if (nextTriple) {
						long tripleKey = 0;
						long tripleValue = 0;

						if (!inverted) {
							tripleKey = value.getSubject();
							tripleValue = value.getObject();
						} else {
							tripleKey = value.getObject();
							tripleValue = value.getSubject();
						}

						Collection<Long> cTriples = schemaTriples.get(tripleKey);
						if (cTriples == null) {
							cTriples = new ArrayList<Long>();
							schemaTriples.put(tripleKey, cTriples);
						}
						cTriples.add(tripleValue);						
					}
				} while (nextTriple); 
			} finally {	
				if (input != null) {		
					input.close();
				}
			}
		}

//		log.info("loadMapIntoMemory's schemaTriples: " + schemaTriples);	// Added by WuGang
		return schemaTriples;
	}	
}