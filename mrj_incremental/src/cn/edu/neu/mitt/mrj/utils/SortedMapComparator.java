/**
 * 
 */
package cn.edu.neu.mitt.mrj.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author gibeon
 *
 */
public class SortedMapComparator extends WritableComparator {
	private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

	protected SortedMapComparator(Class<? extends WritableComparable> keyClass) {
		super(keyClass);
		// TODO Auto-generated constructor stub
	}
	
	public SortedMapComparator(){
		super(Text.class);
	}

	@Override
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
			int arg5) {
		
		SortedMapWritable smw1 = new SortedMapWritable();
		SortedMapWritable smw2 = new SortedMapWritable();
		try {
			ByteArrayInputStream bais1 = new ByteArrayInputStream(arg0, arg1, arg2);
			smw1.readFields(new DataInputStream(bais1));
			ByteArrayInputStream bais2 = new ByteArrayInputStream(arg3, arg4, arg5);
			smw2.readFields(new DataInputStream(bais2));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		System.out.println("smw1.keySet() is: " + smw1.keySet());
//		System.out.println("smw2.keySet() is: " + smw2.keySet());
		
		return smw1.keySet().toString().compareTo(smw2.keySet().toString());
		
//		return super.compare(smw1.toString(), smw2.toString());
		
//		return TEXT_COMPARATOR.compare(smw1.toString(), smw2.toString());
//		return compare(smw1, smw2);
		
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		if ((a instanceof SortedMapWritable) && (b instanceof SortedMapWritable)){
			return super.compare(a.toString(), b.toString());
		}
		return super.compare(a, b);
	}

//	public SortedMapComparator (Class<? extends SortedMapWritable> keyClass){
//		
//	}

}
