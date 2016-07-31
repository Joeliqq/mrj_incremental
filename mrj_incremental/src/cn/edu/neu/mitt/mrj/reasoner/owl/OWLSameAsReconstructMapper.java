package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;

public class OWLSameAsReconstructMapper extends Mapper<LongWritable, BytesWritable, BytesWritable, BytesWritable> {
	
	protected static Logger log = LoggerFactory.getLogger(OWLSameAsReconstructMapper.class);

	private BytesWritable oKey = new BytesWritable();
	private BytesWritable oValue = new BytesWritable();
//	private byte[] bValue = new byte[9];
	private byte[] bValue = new byte[9+8];	//��һ��Long�����ڱ����滻ǰ��resource

	public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
		oKey.set(value.getBytes(), 1, 13);	//��owl:sameas��Ԫ������������tripleId+key.getStep()+key.getDerivation()
		bValue[0] = value.getBytes()[0];	//���ܵ�ֵ��0,1,2,3,4��4�Ǳ�ʾν����owl:sameas������Ϊ��owl:sameas������0��ʾ���1��ʾν�2��3��ʾ����
		
		long resource = NumberUtils.decodeLong(value.getBytes(), 15);	// Added by Wugang, ��value�л��δ�滻ǰ��resource����14��byte�ӵ�
		
		NumberUtils.encodeLong(bValue, 1, key.get());	//�����1�ֽڿ�ʼ����owlsameas�������0�ֽڱ�������ڱ������滻��λ�ã����ܵ�ֵ��0,1,2,3,4��4�Ǳ�ʾν����owl:sameas������Ϊ��owl:sameas������0��ʾ���1��ʾν�2��3��ʾ���
		
		NumberUtils.encodeLong(bValue, 9, resource);	// Added by Wugang, д���滻ǰ��resource��oValue��
		
		// ʵ���������˸���key��value�Ľ�������Ϊ֮ǰvalue�б��������Ԫ���id��������Ϳ�����Ϊkey��
		// �����Ϳ�����reduce�����еõ�����һ����Ԫ�飨��key�е�id��ʶ������ȫ�滻�������Ԫ����
		context.write(oKey, oValue);
	}
	
	@Override
	public void setup(Context context) {

		oValue = new BytesWritable(bValue);
	}
}
