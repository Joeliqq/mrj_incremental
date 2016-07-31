package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

public class OWLAllSomeValuesMapper extends Mapper<Long, Row, BytesWritable, BytesWritable> {

	private static Logger log = LoggerFactory.getLogger(OWLAllSomeValuesMapper.class);
	
	private byte[] bKey = new byte[17];
	private BytesWritable oKey = new BytesWritable(bKey);
	
	// Modified by WuGang����Ҫ���⴫��һ��long���洢�����Ϣ,һ��byte��������someValues(0)����allValues(1)
	private byte[] bValue = new byte[18]; //new byte[9];	
	private BytesWritable oValue = new BytesWritable(bValue);
	
	private static Map<Long, Collection<byte[]>> allValues = null;
	private static Map<Long, Collection<byte[]>> someValues = null;
	private static Set<Long> onPropertyAll = null;
	private static Set<Long> onPropertySome = null;
	private int previousDerivation = -1;
	
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
		//ASS
//		log.info("I'm in OWLAllSomeValuesMapper");
		int step = row.getInt(CassandraDB.COLUMN_INFERRED_STEPS);
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

		if (value.getPredicate() == TriplesUtils.RDF_TYPE
				&& step >= previousDerivation) {
			//DEL
//			log.info("And I met a triple with RDF_TYPE as predicate: " + value);
		
			// ��Ҫ���⴫��һ��w
			if (someValues.containsKey(value.getObject())) {	//�ҵ���һ��(x,rdf:type,w)��������Ԫ�飬����w����v owl:someValuesFrom w
				log.info("I met someValuesFrom: " + value);
				Collection<byte[]> values = someValues.get(value.getObject());
				Iterator<byte[]> itr = values.iterator();
				bKey[0] = 2;
				bValue[0] = 1;
				bValue[17] = 0;	// ��������һ��someValues
				NumberUtils.encodeLong(bKey, 9, value.getSubject());		
				NumberUtils.encodeLong(bValue, 9, value.getObject());	//Added by WuGang, ��wд��value��
				while (itr.hasNext()) {
					byte[] bytes = itr.next();
					System.arraycopy(bytes, 0, bKey, 1, 8);
					System.arraycopy(bytes, 8, bValue, 1, 8);
					context.write(oKey, oValue);	//�������((p,x),v) -> ((p,x),(v,w,0))
				}
			}
			
			// ��Ҫ���⴫��һ��v
			if (allValues.containsKey(value.getObject())) {	//�ҵ���һ��(w,rdf:type,v)��������Ԫ�飬����v����v owl:allValuesFrom u
				log.info("I met allValuesFrom: " + value);
				Collection<byte[]> values = allValues.get(value.getObject());
				Iterator<byte[]> itr = values.iterator();
				bKey[0] = 1;
				bValue[0] = 1;
				bValue[17] = 1;	// ��������һ��allValues
				NumberUtils.encodeLong(bKey, 9, value.getSubject());	
				NumberUtils.encodeLong(bValue, 9, value.getObject());	//Added by WuGang, ��vд��value��
				while (itr.hasNext()) {
					byte[] bytes = itr.next();
					System.arraycopy(bytes, 0, bKey, 1, 8);
					System.arraycopy(bytes, 8, bValue, 1, 8);
					context.write(oKey, oValue);	//�������((p,w),u) -> ((p,w),(u,v,1))
				}
			}

		} else {
			// onPropertySome�з��õ������е���onPropertySome�����õ����ԣ������е� v owl:someValuesFrom w��ʽ����Ԫ���е�w
			if (onPropertySome.contains(value.getPredicate())) {//ĳһ����Ԫ��u p x�е�p��һ��onPropertySome�����õ�������
				//Rule 15 - someValuesFrom
				log.info("I met onPropertySome: " + value);
				bKey[0] = 2;
				bValue[0] = 0;
				NumberUtils.encodeLong(bKey, 1, value.getPredicate());
				NumberUtils.encodeLong(bKey, 9, value.getObject());			
				NumberUtils.encodeLong(bValue, 1, value.getSubject());
				context.write(oKey, oValue);	//�����((p,x),(u,,))	value�ĺ�����field������ֵ,û�и�ֵ
			}
			
			// onPropertyAll�з��õ������е���onPropertyAll�����õ����ԣ������е� v owl:allValuesFrom u��ʽ����Ԫ���е�u
			if (onPropertyAll.contains(value.getPredicate())) {//ĳһ����Ԫ��w p x�е�p��һ��onPropertyAll�����õ�������
				//Rule 16 - allValuesFrom
				log.info("I met onPropertyAll: " + value);
				bKey[0] = 1;
				bValue[0] = 0;	// Added by WuGang, ԭ������������һ�У����ж����������Ԫ��ʱ�������<w p x>������reduce�����л���ɴ���
				NumberUtils.encodeLong(bKey, 1, value.getPredicate());
				NumberUtils.encodeLong(bKey, 9, value.getSubject());
				NumberUtils.encodeLong(bValue, 1, value.getObject());
				context.write(oKey, oValue);	//�������((p,w),(x,,))	value�ĺ�����field������ֵ,û�и�ֵ
			}
		}
	}
	
	@Override
	public void setup(Context context) throws IOException {
		previousDerivation = context.getConfiguration().getInt("reasoner.previousDerivation", -1);

//		List<FileStatus> filesProperty =  MultiFilesReader.recursiveListStatus(context, "FILTER_ONLY_OWL_ON_PROPERTY");		
//		Map<Long,Collection<Long>> allValuesTmp = FilesTriplesReader.loadMapIntoMemory("FILTER_ONLY_OWL_ALL_VALUES", context);
//		Map<Long,Collection<Long>> someValuesTmp = FilesTriplesReader.loadMapIntoMemory("FILTER_ONLY_OWL_SOME_VALUES", context);
		
		CassandraDB db;
		try{
			db = new CassandraDB();
			
			Set<Integer> filters = new HashSet<Integer>();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_ON_PROPERTY);
			Map<Long,Collection<Long>> onPropertyTmp = db.loadMapIntoMemory(filters);
			
			filters.clear();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_ALL_VALUES_FROM);
			Map<Long,Collection<Long>> allValuesTmp = db.loadMapIntoMemory(filters);
			
			filters.clear();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_SOME_VALUES_FROM);
			Map<Long,Collection<Long>> someValuesTmp = db.loadMapIntoMemory(filters);
			
			onPropertyAll = new HashSet<Long>();
			onPropertySome = new HashSet<Long>();
			someValues = new HashMap<Long, Collection<byte[]>>();
			allValues = new HashMap<Long, Collection<byte[]>>();
			makeJoin(onPropertyTmp, context, someValuesTmp, 
					allValuesTmp, someValues, allValues,
					onPropertySome, onPropertyAll);
		
		
			db.CassandraDBClose();

			
		}catch (TTransportException e) {
			e.printStackTrace();
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	
		log.info("After setup, allValuesFrom is: " + allValues);
	}
	
	protected void makeJoin(Map<Long,Collection<Long>> onPropertyTmp, Context context,
			Map<Long, Collection<Long>> someValuesTmp, 
			Map<Long, Collection<Long>> allValuesTmp,
			Map<Long, Collection<byte[]>> someValues,
			Map<Long, Collection<byte[]>> allValues,
			Set<Long> onPropertySome,
			Set<Long> onPropertyAll) {
//		TripleSource key = new TripleSource();
//		Triple value = new Triple();
		
		for (Entry<Long, Collection<Long>> entry : onPropertyTmp.entrySet()){
			Long sub = entry.getKey();
			Collection<Long> objects = entry.getValue();
			for (Long obj : objects){

				//Check if there is a match with someValuesFrom and allValuesFrom
				if (someValuesTmp.containsKey(sub)) {
					Collection<Long> col = someValuesTmp.get(sub);
					if (col != null) {
						Iterator<Long> itr = col.iterator();
						while (itr.hasNext()) {
							long w = itr.next();
							byte[] bytes = new byte[16];
							//Save v and p there.
							NumberUtils.encodeLong(bytes, 0, obj);
							NumberUtils.encodeLong(bytes, 8, sub);
							
							Collection<byte[]> cValues = someValues.get(w);
							if (cValues == null) {
								cValues = new ArrayList<byte[]>();
								someValues.put(w, cValues);
							}
							cValues.add(bytes);
							onPropertySome.add(obj);
						}
					}
				}
				
				if (allValuesTmp.containsKey(sub)) {
					// col��Ӧ�������е�subject��Ӧ��object������subject��object����subject, owl:allValuesFrom, object
					Collection<Long> col = allValuesTmp.get(sub);	
					if (col != null) {
						Iterator<Long> itr = col.iterator();
						while (itr.hasNext()) {
							long w = itr.next();
							byte[] bytes = new byte[16];
							//Save v and p there.
							NumberUtils.encodeLong(bytes, 0, obj);
							NumberUtils.encodeLong(bytes, 8, w);
							
							Collection<byte[]> cValues = allValues.get(sub);
							if (cValues == null) {
								cValues = new ArrayList<byte[]>();
								allValues.put(sub, cValues);
							}
							cValues.add(bytes);
							onPropertyAll.add(obj);
						}
					}
				}
			}
		}
		
	}

}
