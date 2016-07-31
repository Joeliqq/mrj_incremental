/**
 * 
 */
package cn.edu.neu.mitt.mrj.justification;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

/**
 * @author gibeon
 *
 */
public class OWLHorstJustificationMapper extends
		Mapper<Triple, MapWritable, MapWritable, LongWritable> {
	
	private static Logger log = LoggerFactory.getLogger(OWLHorstJustificationMapper.class);
	
	CassandraDB db = null;

	
	
	@Override
	protected void map(Triple key, MapWritable value, Context context)
			throws IOException, InterruptedException {
		try {
			// Create a toExtendExplanations without the key triple 
			MapWritable toExtendExplanations = new MapWritable();
			for (Writable triple : value.keySet()){
				// Find the triple to be traced.
				if ((((Triple)triple).getSubject() == key.getSubject()) &&
						(((Triple)triple).getPredicate() == key.getPredicate()) &&
						(((Triple)triple).getObject() == key.getObject()) &&
						(((Triple)triple).isObjectLiteral() == key.isObjectLiteral())){
					// do nothing
				}else
					toExtendExplanations.put((Triple)triple, NullWritable.get());
			}

			
			Set<Triple> tracingEntries = db.getTracingEntries(key);

			// This explanation cannot be traced from this key triple
			if (tracingEntries.size() == 0){
				context.write(value, new LongWritable(1));
//				System.out.println("The key triple cannot be traced: " + key);
			}
			
			for (Triple tracingEntry : tracingEntries){
				Set<Triple> tracedTriples = tracing(tracingEntry);
				MapWritable newExplanation = new MapWritable(toExtendExplanations);
				for (Triple tracedTriple : tracedTriples)
					newExplanation.put(tracedTriple, NullWritable.get());
				// This explanation may be further traced from this key triple
				context.write(newExplanation, new LongWritable(0));
//				System.out.println("The key triple can be traced: " + key + "; the new explanation is: ");
				for (Writable t : newExplanation.keySet()){
					System.out.println(t);
				}
			}
			
		} catch (InvalidRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private Set<Triple> tracing(Triple triple){
		Set<Triple> tracedTriples = new HashSet<Triple>();

		long type = triple.getType();
		switch ((int) type) {
		case (int) TriplesUtils.OWL_HORST_1: 
		{
			// p rdf:type owl:FunctionalProperty, [u p v], u p w => v owl:sameAs w
			// <v, owl:sameAs, w, OWL_HORST_1, u, p, v>
			Triple triple1 = new Triple(triple.getRpredicate(),	TriplesUtils.RDF_TYPE, TriplesUtils.OWL_FUNCTIONAL_PROPERTY, false);
			Triple triple2 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			Triple triple3 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getObject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
			tracedTriples.add(triple3);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_2:
		{
			// p rdf:type owl:InverseFunctionalProperty, [v p u], w p u => v owl:sameAs w
			// <v owl:sameAs, w, OWL_HORST_2, v, p, u>
			Triple triple1 = new Triple(triple.getRpredicate(),	TriplesUtils.RDF_TYPE, TriplesUtils.OWL_INVERSE_FUNCTIONAL_PROPERTY, false);
			Triple triple2 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			Triple triple3 = new Triple(triple.getObject(), triple.getRpredicate(), triple.getRobject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
			tracedTriples.add(triple3);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_3:
		{
			// p rdf:type owl:SymmetricProperty, [v p u] => u p v
			// <u, p, v, OWL_HORST_3, v, p, u>
			Triple triple1 = new Triple(triple.getPredicate(),	TriplesUtils.RDF_TYPE, TriplesUtils.OWL_SYMMETRIC_PROPERTY, false);
			Triple triple2 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_4:
		{
			// p rdf:type owl:TransitiveProperty, [u p w], w p v => u p v
			// <u, p, v, OWL_HORST_4, u, p, w>
			Triple triple1 = new Triple(triple.getPredicate(),	TriplesUtils.RDF_TYPE, TriplesUtils.OWL_TRANSITIVE_PROPERTY, false);
			Triple triple2 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			Triple triple3 = new Triple(triple.getRobject(), triple.getPredicate(), triple.getObject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
			tracedTriples.add(triple3);
//			System.out.println("I'm OWL_HORST_4: " + triple + derivedTriples);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_5a:
			log.error("OWL Horst Rule 5a is not implemented!");
			break;
		case (int) TriplesUtils.OWL_HORST_5b:
			log.error("OWL Horst Rule 5b is not implemented!");
			break;
		case (int) TriplesUtils.OWL_HORST_6:
			log.error("OWL Horst Rule 6 is not implemented!");
			break;
		case (int) TriplesUtils.OWL_HORST_7:
		{	// [v owl:sameAs w], w owl:sameAs u => v owl:sameAs u
			// <v, owl:sameAs, u, OWL_HORST_7, v, owl:sameAs, w>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.OWL_SAME_AS, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRobject(), TriplesUtils.OWL_SAME_AS, triple.getObject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_8:
		{	// p owl:inverseOf q, [v p w] => w q v  // OWL_HORST_8a
			// <w, q, v, OWL_HORST_8, v, p, w>
			// p owl:inverseOf q, [v q w] => w p v  // OWL_HORST-8b
			// <w, p, v, OWL_HORST_8, v, q, w>
			Triple triple1 = new Triple(triple.getRpredicate(), TriplesUtils.OWL_INVERSE_OF, triple.getPredicate(), false);
			Triple triple2 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_8a:
			log.error("OWL Horst Rule 8a is not implemented!");
			break;
		case (int) TriplesUtils.OWL_HORST_8b:
			log.error("OWL Horst Rule 8b is not implemented!");
			break;
		case (int) TriplesUtils.OWL_HORST_9:
			log.error("OWL Horst Rule 9 is not implemented!");
			break;
		case (int) TriplesUtils.OWL_HORST_10:
			log.error("OWL Horst Rule 10 is not implemented!");
			break;
		case (int) TriplesUtils.OWL_HORST_11:
		{	// [u p v], u owl:sameAs x, v owl:sameAs y => x p y
			// <x, p, y, OWL_HORST_11, u, p, v>
			Triple triple1 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRsubject(), TriplesUtils.OWL_SAME_AS, triple.getSubject(), false);
			Triple triple3 = new Triple(triple.getRobject(), TriplesUtils.OWL_SAME_AS, triple.getObject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
			tracedTriples.add(triple3);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_12a:
		{	// [v owl:equivalentClass w] => v rdfs:subClassOf w
			// <v, rdfs:subClassOf, w, OWL_HORST_12a, v, owl:equivalentClass, w>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.OWL_EQUIVALENT_CLASS, triple.getRobject(), false);
			tracedTriples.add(triple1);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_12b:
		{	// [v owl:equivalentClass w] => w rdfs:subClassOf v
			// <w, rdfs:subClassOf, v, OWL_HORST_12b, v, owl:equivalentClass, w>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.OWL_EQUIVALENT_CLASS, triple.getRobject(), false);
			tracedTriples.add(triple1);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_12c:
		{	// [v rdfs:subClassOf w], w rdfs:subClassOf v => v rdfs:equivalentClass w
			// <v, rdfs:equivalentClass, w, OWL_HORST_12c, v, rdfs:subClassOf, w>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.RDFS_SUBCLASS, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRobject(), TriplesUtils.RDFS_SUBCLASS, triple.getRsubject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_13a:
		{	// [v owl:equivalentProperty w] => v rdfs:subPropertyOf w
			// <v, rdfs:subPropertyOf, w, OWL_HORST_13a, v, owl:equivalentProperty, w>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.OWL_EQUIVALENT_PROPERTY, triple.getRobject(), false);
			tracedTriples.add(triple1);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_13b:
		{	// [v owl:equivalentProperty w] => w rdfs:subPropertyOf v
			// <w, rdfs:subPropertyOf, v, OWL_HORST_13b, v, owl:equivalentProperty, w>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.OWL_EQUIVALENT_PROPERTY, triple.getRobject(), false);
			tracedTriples.add(triple1);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_13c:
		{	// [v rdfs:subPropertyOf w], w rdfs:subPropertyOf v => v rdfs:equivalentProperty w
			// <v, rdfs:equivalentProperty, w, OWL_HORST_13c, v, rdfs:subPropertyOf, w>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.RDFS_SUBPROPERTY, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRobject(), TriplesUtils.RDFS_SUBPROPERTY, triple.getRsubject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_14a:
		{	// [v] owl:hasValue [w], v owl:onProperty [p], u p w => u rdf:type v
			// <u, rdf:type, v, OWL_HORST_14a, v, p, w>
			Triple triple1 = new Triple(triple.getObject(), TriplesUtils.OWL_HAS_VALUE, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getObject(), TriplesUtils.OWL_ON_PROPERTY, triple.getRpredicate(), false);
			Triple triple3 = new Triple(triple.getSubject(), triple.getRpredicate(), triple.getRobject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
			tracedTriples.add(triple3);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_14b:
		{	// [v owl:hasValue w], v owl:onProperty p, u rdf:type v => u p w
			// <u, p, w, OWL_HORST_14b, v, owl:hasValue, w>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.OWL_HAS_VALUE, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRsubject(), TriplesUtils.OWL_ON_PROPERTY, triple.getPredicate(), false);
			Triple triple3 = new Triple(triple.getSubject(), TriplesUtils.RDF_TYPE, triple.getRsubject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
			tracedTriples.add(triple3);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_15:
		{
			// v owl:someValuesFrom w, v owl:onProperty [p], u p x, [x] rdf:type [w] => u rdf:type v
			// <u, rdf:type, v, OWL_HORST_15, x, p, w>
			Triple triple1 = new Triple(triple.getObject(), TriplesUtils.OWL_SOME_VALUES_FROM, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getObject(), TriplesUtils.OWL_ON_PROPERTY, triple.getRpredicate(), false);
			Triple triple3 = new Triple(triple.getSubject(), triple.getRpredicate(), triple.getRsubject(), false);
			Triple triple4 = new Triple(triple.getRsubject(), TriplesUtils.RDF_TYPE, triple.getRobject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
			tracedTriples.add(triple3);
			tracedTriples.add(triple4);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_16:
		{
			// v owl:allValuesFrom u, v owl:onProperty [p], [w] rdf:type [v], w p x => x rdf:type u
			// <x, rdf:type, u, OWL_HORST_16, w, p, v>
			Triple triple1 = new Triple(triple.getRobject(), TriplesUtils.OWL_ALL_VALUES_FROM, triple.getObject(), false);
			Triple triple2 = new Triple(triple.getRobject(), TriplesUtils.OWL_ON_PROPERTY, triple.getRpredicate(), false);
			Triple triple3 = new Triple(triple.getRsubject(), TriplesUtils.RDF_TYPE, triple.getRobject(), false);
			Triple triple4 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getSubject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
			tracedTriples.add(triple3);
			tracedTriples.add(triple4);
//			System.out.println("I'm OWL_HORST_16: " + triple + derivedTriples);
		}
			break;
		case (int) TriplesUtils.RDFS_NA:
			log.error("This RDFS rule is not implemented!");
			break;
		case (int) TriplesUtils.RDFS_1:
			log.error("RDFS Rule 1 is not implemented!");
			break;
		case (int) TriplesUtils.RDFS_2:
		{	// p rdfs:domain x, [s p o] => s rdf:type x
			// <s, rdf:type, x, RDFS_2, s, p, o>
			Triple triple1 = new Triple(triple.getRpredicate(), TriplesUtils.RDFS_DOMAIN, triple.getObject(), false);
			Triple triple2 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.RDFS_3:
		{	// p rdfs:range x, [s p o] => o rdf:type x
			// <o, rdf:type, x, RDFS_3, s, p, o>
			Triple triple1 = new Triple(triple.getRpredicate(), TriplesUtils.RDFS_RANGE, triple.getObject(), false);
			Triple triple2 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.RDFS_4a:
			log.error("RDFS Rule 4a is not implemented!");
			break;
		case (int) TriplesUtils.RDFS_4b:
			log.error("RDFS Rule 4b is not implemented!");
			break;
		case (int) TriplesUtils.RDFS_5:
		{	// [p rdfs:subPropertyOf q], q rdfs:subPropertyOf r => p rdfs:subPropertyOf r
			// <p, rdfs:subPropertyOf, r, RDFS_5, p, rdfs:subPropertyOf, q>
			Triple triple1 = new Triple(triple.getSubject(), TriplesUtils.RDFS_SUBPROPERTY, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRobject(), TriplesUtils.RDFS_SUBPROPERTY, triple.getObject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.RDFS_6:
			log.error("RDFS Rule 6 is not implemented!");
			break;
		case (int) TriplesUtils.RDFS_7:
		{	// [s p o], p rdfs:subPropertyOf q => s q o
			// <s, q, o, RDFS_7, s, p, o>
			Triple triple1 = new Triple(triple.getRsubject(), triple.getRpredicate(), triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRpredicate(), TriplesUtils.RDFS_SUBPROPERTY, triple.getPredicate(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.RDFS_8:
		{	// [s rdf:type rdfs:Class] => s rdfs:subClassOf rdfs:Resource
			// <s, rdfs:subClassOf, rdfs:Resource, RDFS_8, s, rdf:type, rdfs:Class>
			Triple triple1 = new Triple(triple.getSubject(), TriplesUtils.RDF_TYPE, TriplesUtils.RDFS_CLASS, false);
			tracedTriples.add(triple1);
		}
			break;
		case (int) TriplesUtils.RDFS_9:
		{	// [s rdf:type x], x rdfs:subClassOf y => s rdf:type y
			// <s, rdf:type, y, RDFS_9, s, rdf:type, x>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.RDF_TYPE, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRobject(), TriplesUtils.RDFS_SUBCLASS, triple.getObject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.RDFS_10:
			log.error("RDFS Rule 10 is not implemented!");
			break;
		case (int) TriplesUtils.RDFS_11:
		{	// [x rdfs:subClassOf y], y rdfs:subClassOf z => x rdfs:subClassOf z
			// <x, rdfs:subClassOf, z, RDFS_11, x, rdfs:subClassOf, y>
			Triple triple1 = new Triple(triple.getRsubject(), TriplesUtils.RDFS_SUBCLASS, triple.getRobject(), false);
			Triple triple2 = new Triple(triple.getRobject(), TriplesUtils.RDFS_SUBCLASS, triple.getObject(), false);
			tracedTriples.add(triple1);
			tracedTriples.add(triple2);
		}
			break;
		case (int) TriplesUtils.RDFS_12:
		{	// [p rdf:type rdfs:ContainerMembershipProperty] => p rdfs:subPropertyOf rdfs:member
			// <p, rdfs:subPropertyOf, rdfs:member, RDFS_12, p, rdf:type, rdfs:ContainerMembershipProperty>
			Triple triple1 = new Triple(triple.getSubject(), TriplesUtils.RDF_TYPE, TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY, false);
			tracedTriples.add(triple1);
		}
			break;
		case (int) TriplesUtils.RDFS_13:
		{	// [o rdf:type rdfs:Datatype] => o rdfs:subClassOf rdfs:Literal
			// <o, rdfs:subClassOf, rdfs:Literal, RDFS_13, o, rdf:type, rdfs:Datatype>
			Triple triple1 = new Triple(triple.getSubject(), TriplesUtils.RDF_TYPE, TriplesUtils.RDFS_DATATYPE, false);
			tracedTriples.add(triple1);
		}
			break;
		case (int) TriplesUtils.OWL_HORST_NA:
		default:
			log.error("This OWL Horst rule is not implemented!");
			break;
		}
		
		return tracedTriples;
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try{
			db = new CassandraDB();
		}catch(TException te){
			te.printStackTrace();
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		db.CassandraDBClose();
	}

}
