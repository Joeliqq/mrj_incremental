package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;

import com.datastax.driver.core.Row;

public class OWLSameAsMapper extends Mapper<Long, Row, LongWritable, BytesWritable> {

	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	private byte[] bValue = new byte[9];
	
	/* Flags:
	 * 0 : the pair is of the "k" type
	 * 1 : the pair is of the "group" type */
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
//		System.out.println("In OWLSameAsMapper.");
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

		
		/* Source triple: s owl:sameAs o */
		long olKey = 0;
		long olValue = 0;
		if (value.getSubject() > value.getObject()) {	//key�����Ǵ�ֵ��value����Сֵ
			olKey = value.getSubject();
			olValue = value.getObject();
		} else {
			olKey = value.getObject();
			olValue = value.getSubject();
		}
		
		// ����С�Ǹ�ֵ��ʶÿһ����
		
		oKey.set(olKey);
		bValue[0] = 0;
		NumberUtils.encodeLong(bValue, 1, olValue);		
		oValue.set(bValue, 0, bValue.length);
		context.write(oKey, oValue);	//����key�Ǵ�ֵ��value��Сֵ����������Ե�֪ÿһ��resource�������ĸ���
		
		oKey.set(olValue);
		bValue[0] = 1;
		NumberUtils.encodeLong(bValue, 1, olKey);		
		oValue.set(bValue, 0, bValue.length);
		context.write(oKey, oValue);	//����key��Сֵ��value�Ǵ�ֵ����������Ե�֪ÿһ�����а�����Щresource
	}
	public void setup(Context context) throws IOException{

	}
}
