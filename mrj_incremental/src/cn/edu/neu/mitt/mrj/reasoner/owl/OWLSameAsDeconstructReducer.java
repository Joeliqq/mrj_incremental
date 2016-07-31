package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;

public class OWLSameAsDeconstructReducer extends Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	
	private byte[] bOriginalResource = new byte[8];	//��һ��Long���ڴ��ԭʼ���滻ǰ����resource

	
	private List<byte[]> storage = new LinkedList<byte[]>();
	
	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {		
//		System.out.println("In OWLSameAsDeconstructReducer!!!");

		storage.clear();
		long countOutput = 0;
		oKey.set(key.get());
		NumberUtils.encodeLong(bOriginalResource, 0, key.get());	// Added by Wugang
		
		boolean replacement = false;
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable iValue = itr.next();
			byte[] bValue = iValue.getBytes();
//			System.out.println("In processing things before storage, size of iValue is: " + iValue.getLength());
//			System.out.println("In processing things before storage, size of bValue is: " + bValue.length);
			// ���ڲ�һ���ں�ʱ��������bValue[0]=4���Ǹ�key��value��
			// ���������֮ǰ�Ƚ����滻��resource������storage�ﱣ����������һ��������֮��Ϳ���ֱ�ӽ����滻�ˣ���value�滻
			// ���(�����whileѭ��֮��)�ٰ�storage���汣���ֵ�����滻һ�¡�
			if (bValue[0] == 4) {//Same as
				long resource = NumberUtils.decodeLong(bValue, 1);
				replacement = true;
				oKey.set(resource);
			} else if (!replacement && bValue[14] == 0) {
				//Store in memory the results
				byte[] newValue = Arrays.copyOf(bValue, bValue.length);
				storage.add(newValue);
			} else {
				//I already found the replacement. Simply output everything
				byte[] bTempValue = new byte[15+8];	// Added by WuGang
				System.arraycopy(bValue, 0, bTempValue, 0, 15);	// Added by WuGang
				System.arraycopy(bOriginalResource, 0, bTempValue, 15, 8);	// Added by WuGang
				iValue.set(bTempValue, 0, bTempValue.length);	// Added by Wugang, �����д��һ���滻ǰ��resource
				context.write(oKey, iValue);
				countOutput++;
				context.getCounter("reasoner", "substitutions").increment(1);
			}
		}
		
		Iterator<byte[]> itr2 = storage.iterator();	//���storageΪ����˵���������������û�п�����sameas���滻�Ķ���
		while (itr2.hasNext()) {
			byte[] bValue = itr2.next();
			oValue.set(bValue, 0, bValue.length);
			
			byte[] bTempValue = new byte[15+8];	// Added by WuGang
//			System.out.println("In processing things in storage, size of bValue is: " + bValue.length);
			System.arraycopy(bValue, 0, bTempValue, 0, 15);	// Added by WuGang
			System.arraycopy(bOriginalResource, 0, bTempValue, 15, 8);	// Added by WuGang
			oValue.set(bTempValue, 0, bTempValue.length);	// Added by Wugang, �����д��һ���滻ǰ��resource
			context.write(oKey, oValue);
		}
		
		//�����oKey��һ����Դ������sameas����������Ķ����滻���ˣ���ʵ�����Ѿ�����sameas�滻����ˣ�
		//�����oVlaue��owl:sameas��Ԫ������������tripleId+key.getStep()+key.getDerivation()��ǰ�滹��һ��byte
		
		if (replacement) { //Increment counter of replacements
			context.getCounter("reasoner", "substitutions").increment(countOutput + storage.size());
		}
	}
	public void setup(Context context) throws IOException, InterruptedException{
		CassandraDB.setConfigLocation();

	}
}
