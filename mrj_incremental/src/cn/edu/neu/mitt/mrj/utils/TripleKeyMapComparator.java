/**
 * Project Name: mrj-0.1
 * File Name: TripleMapComparator.java
 * @author Gang Wu
 * 2015年2月10日 上午2:31:18
 * 
 * Description: 
 * TODO
 */
package cn.edu.neu.mitt.mrj.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import cn.edu.neu.mitt.mrj.data.Triple;

/**
 * @author gibeo_000
 *
 */
public class TripleKeyMapComparator extends WritableComparator {

	@SuppressWarnings("rawtypes")
	protected TripleKeyMapComparator(Class<? extends WritableComparable> keyClass) {
		super(keyClass);
	}
	
	public TripleKeyMapComparator(){
		super(Text.class);
	}

	@Override
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
			int arg5) {
		MapWritable mw1 = new MapWritable();
		MapWritable mw2 = new MapWritable();
		try {
			ByteArrayInputStream bais1 = new ByteArrayInputStream(arg0, arg1, arg2);
			mw1.readFields(new DataInputStream(bais1));
			ByteArrayInputStream bais2 = new ByteArrayInputStream(arg3, arg4, arg5);
			mw2.readFields(new DataInputStream(bais2));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Set<Triple> tripleSet1 = new HashSet<Triple>(); 
		for(Writable w : mw1.keySet()){
			Triple targetTriple = new Triple(); 
			Triple originalTriple = (Triple) w;
			targetTriple.setSubject(originalTriple.getSubject());
			targetTriple.setPredicate(originalTriple.getPredicate());
			targetTriple.setObject(originalTriple.getObject());
		}
		Set<Triple> tripleSet2 = new HashSet<Triple>(); 
		for(Writable w : mw2.keySet()){
			Triple targetTriple = new Triple(); 
			Triple originalTriple = (Triple) w;
			targetTriple.setSubject(originalTriple.getSubject());
			targetTriple.setPredicate(originalTriple.getPredicate());
			targetTriple.setObject(originalTriple.getObject());
		}
		
		return tripleSet1.toString().compareTo(tripleSet2.toString());
	}

	@Override
	public int compare(Object a, Object b) {
		if ((a instanceof MapWritable) && (b instanceof MapWritable)){
			return ((MapWritable)a).keySet().toString().compareTo(((MapWritable)b).keySet().toString());
		}
		return super.compare(a, b);
	}
	
}
