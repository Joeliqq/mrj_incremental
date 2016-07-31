package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;

public class OWLSameAsReconstructReducer extends Reducer<BytesWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

	private TripleSource oKey = new TripleSource();
	private Triple oValue = new Triple();

	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
//		System.out.println("In OWLSameAsReconstructReducer!!!");
		byte[] bKey = key.getBytes();		
		oKey.setStep(NumberUtils.decodeInt(bKey, 8));
		oKey.setDerivation(bKey[12]);
		
		int elements = 0;
		Iterator<BytesWritable> itr = values.iterator();	////�����1�ֽڿ�ʼ����owlsameas�������0�ֽڱ�������ڱ������滻��λ�ã����ܵ�ֵ��0,1,2,3,4��4�Ǳ�ʾν����owl:sameas������Ϊ��owl:sameas������0��ʾ���1��ʾν�2��3��ʾ���
		while (itr.hasNext()) {
			elements++;
			byte[] bValue = itr.next().getBytes();
			long resource = NumberUtils.decodeLong(bValue, 1);	//�������owlsameas��������潫���������滻��
			long originalResource = NumberUtils.decodeLong(bValue, 9);	// Added by Wugang, �����滻ǰ��resource
			switch (bValue[0]) {
				case 0:
					oValue.setSubject(resource);	//�滻����
					oValue.setRsubject(originalResource);	// Added by Wugang, ԭʼ����
//					System.out.println("Replacing subject: "  + resource);
					break;
				case 1:
					oValue.setPredicate(resource);	//�滻ν��
					oValue.setRpredicate(originalResource);	// Added by Wugang, ԭʼν��
//					System.out.println("Replacing predicate: "  + resource);
					break;
				case 2:								//�滻����
				case 3:								//�滻����
					if (bValue[0] == 2)
						oValue.setObjectLiteral(false);
					else
						oValue.setObjectLiteral(true);
					oValue.setObject(resource);			
					oValue.setRobject(originalResource);	// Added by Wugang, ԭʼ����
//					System.out.println("Replacing object: "  + resource);
					break;
				default:
					break;
			}
		}
		
		if (elements == 3){
			// Added by WuGang, ���rule11
//			oValue.setRsubject(rsubject)
			if ((oValue.getSubject() == oValue.getRsubject())
					&& (oValue.getPredicate() == oValue.getRpredicate())
					&& (oValue.getObject() == oValue.getRobject()))
				oValue.setType(TriplesUtils.OWL_HORST_NA); // �滻ǰ��û�б仯������һ��sameasrule
			else {

				if ((oValue.getPredicate() == TriplesUtils.OWL_SAME_AS)
						&& (oValue.getRpredicate() == TriplesUtils.OWL_SAME_AS))
					oValue.setType(TriplesUtils.OWL_HORST_7); // Ӧ��OWL Horst����7
				else
					oValue.setType(TriplesUtils.OWL_HORST_11); // Ӧ��OWL
																// Horst����11
			}
			
//			System.out.println("Find a complete replacment of triple: " + oValue);
			CassandraDB.writeJustificationToMapReduceContext(oValue, oKey, context); 		
//			context.write(oKey, oValue);
		}
	}
	
	@Override
	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

	}

	@Override
	protected void cleanup(
			Reducer<BytesWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
