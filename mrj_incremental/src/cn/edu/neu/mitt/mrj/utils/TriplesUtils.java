package cn.edu.neu.mitt.mrj.utils;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;

public class TriplesUtils {
	
	private static TriplesUtils instance = null;
	
	private Map<String, Long> preloadedURIs = null;
	
	// Added by WuGang 2010-12-1, 反向的preloadedURIs
	private Map<Long, String> preloadedResources = null;
	
	private static Logger log = LoggerFactory.getLogger(TriplesUtils.class);
	
	/***** RDFS 规则号 *****/
	public static final long RDFS_NA = 0xf0;	// 不可用的rule
	public static final long RDFS_1 = 0xf1;
	public static final long RDFS_2 = 0xf2;
	public static final long RDFS_3 = 0xf3;
	public static final long RDFS_4a = 0xf4a;
	public static final long RDFS_4b = 0xf4b;
	public static final long RDFS_5 = 0xf5;
	public static final long RDFS_6 = 0xf6;
	public static final long RDFS_7 = 0xf7;
	public static final long RDFS_8 = 0xf8;	
	public static final long RDFS_9 = 0xf9;
	public static final long RDFS_10 = 0xf10;	
	public static final long RDFS_11 = 0xf11;	
	public static final long RDFS_12 = 0xf12;	
	public static final long RDFS_13 = 0xf13;	
	
	/***** OWL Horst 规则标号 *****/
	public static final long OWL_HORST_NA = 0x0;	// 不可用的rule
	public static final long OWL_HORST_1 = 0x1;
	public static final long OWL_HORST_2 = 0x2;
	public static final long OWL_HORST_3 = 0x3;
	public static final long OWL_HORST_4 = 0x4;
	public static final long OWL_HORST_5a = 0x5a;
	public static final long OWL_HORST_5b = 0x5b;
	public static final long OWL_HORST_6 = 0x6;
	public static final long OWL_HORST_7 = 0x7;
	public static final long OWL_HORST_8 = 0x8;	//很难判断是8a或者8b，因此用一个统称
	public static final long OWL_HORST_8a = 0x8a;
	public static final long OWL_HORST_8b = 0x8b;
	public static final long OWL_HORST_9 = 0x9;
	public static final long OWL_HORST_10 = 0x10;
	public static final long OWL_HORST_11 = 0x11;
//	public static final long OWL_HORST_12ab = 0x12ab;	//很难判断是12a或者12b，因此用一个统称
	public static final long OWL_HORST_12a = 0x12a;
	public static final long OWL_HORST_12b = 0x12b;
	public static final long OWL_HORST_12c = 0x12c;
//	public static final long OWL_HORST_13ab = 0x13ab;	//很难判断是13a或者13b，因此用一个统称
	public static final long OWL_HORST_13a = 0x13a;
	public static final long OWL_HORST_13b = 0x13b;
	public static final long OWL_HORST_13c = 0x13c;
	public static final long OWL_HORST_14a = 0x14a;
	public static final long OWL_HORST_14b = 0x14b;
	public static final long OWL_HORST_15 = 0x15;
	public static final long OWL_HORST_16 = 0x16;
	// Added by Wugang 20150108, same as synonyms table, related to 5,6,7,9,10.11
	public static final long OWL_HORST_SYNONYMS_TABLE = 0x56791011;	 

	
	/***** Standard URIs IDs *****/
	public static final long RDF_TYPE = 0;
	public static final long RDF_PROPERTY = 1;
	public static final long RDFS_RANGE = 2;
	public static final long RDFS_DOMAIN = 3;
	public static final long RDFS_SUBPROPERTY = 4;
	public static final long RDFS_SUBCLASS = 5;
	public static final long RDFS_MEMBER = 19;
	public static final long RDFS_LITERAL = 20;
	public static final long RDFS_CONTAINER_MEMBERSHIP_PROPERTY = 21;
	public static final long RDFS_DATATYPE = 22;
	public static final long RDFS_CLASS = 23;
	public static final long RDFS_RESOURCE = 24;
	public static final long OWL_CLASS = 6;
	public static final long OWL_FUNCTIONAL_PROPERTY = 7;
	public static final long OWL_INVERSE_FUNCTIONAL_PROPERTY = 8;
	public static final long OWL_SYMMETRIC_PROPERTY = 9;
	public static final long OWL_TRANSITIVE_PROPERTY = 10;
	public static final long OWL_SAME_AS = 11;
	public static final long OWL_INVERSE_OF = 12;
	public static final long OWL_EQUIVALENT_CLASS = 13;
	public static final long OWL_EQUIVALENT_PROPERTY = 14;
	public static final long OWL_HAS_VALUE = 15;
	public static final long OWL_ON_PROPERTY = 16;
	public static final long OWL_SOME_VALUES_FROM = 17;
	public static final long OWL_ALL_VALUES_FROM = 18;
	
	/***** Standard URIs *****/
	public static final String S_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
	public static final String S_RDF_PROPERTY = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>";
	public static final String S_RDFS_RANGE = "<http://www.w3.org/2000/01/rdf-schema#range>";
	public static final String S_RDFS_DOMAIN = "<http://www.w3.org/2000/01/rdf-schema#domain>";
	public static final String S_RDFS_SUBPROPERTY = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
	public static final String S_RDFS_SUBCLASS = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
	public static final String S_RDFS_MEMBER = "<http://www.w3.org/2000/01/rdf-schema#member>";
	public static final String S_RDFS_LITERAL = "<http://www.w3.org/2000/01/rdf-schema#Literal>";
	public static final String S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY = "<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty>";
	public static final String S_RDFS_DATATYPE = "<http://www.w3.org/2000/01/rdf-schema#Datatype>";
	public static final String S_RDFS_CLASS = "<http://www.w3.org/2000/01/rdf-schema#Class>";
	public static final String S_RDFS_RESOURCE = "<http://www.w3.org/2000/01/rdf-schema#Resource>";
	public static final String S_OWL_CLASS = "<http://www.w3.org/2002/07/owl#Class>";
	public static final String S_OWL_FUNCTIONAL_PROPERTY = "<http://www.w3.org/2002/07/owl#FunctionalProperty>";
	public static final String S_OWL_INVERSE_FUNCTIONAL_PROPERTY = "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>";
	public static final String S_OWL_SYMMETRIC_PROPERTY = "<http://www.w3.org/2002/07/owl#SymmetricProperty>";
	public static final String S_OWL_TRANSITIVE_PROPERTY = "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
	public static final String S_OWL_SAME_AS = "<http://www.w3.org/2002/07/owl#sameAs>";
	public static final String S_OWL_INVERSE_OF = "<http://www.w3.org/2002/07/owl#inverseOf>";
	public static final String S_OWL_EQUIVALENT_CLASS = "<http://www.w3.org/2002/07/owl#equivalentClass>";
	public static final String S_OWL_EQUIVALENT_PROPERTY = "<http://www.w3.org/2002/07/owl#equivalentProperty>";
	public static final String S_OWL_HAS_VALUE = "<http://www.w3.org/2002/07/owl#hasValue>";
	public static final String S_OWL_ON_PROPERTY = "<http://www.w3.org/2002/07/owl#onProperty>";
	public static final String S_OWL_SOME_VALUES_FROM = "<http://www.w3.org/2002/07/owl#someValuesFrom>";
	public static final String S_OWL_ALL_VALUES_FROM = "<http://www.w3.org/2002/07/owl#allValuesFrom>";

	/***** TRIPLES TYPES *****/
	//USED IN RDFS REASONER
	public static final int DATA_TRIPLE = 0;
	public static final int DATA_TRIPLE_TYPE = 5;
	public static final int SCHEMA_TRIPLE_RANGE_PROPERTY = 1;
	public static final int SCHEMA_TRIPLE_DOMAIN_PROPERTY = 2;
	public static final int SCHEMA_TRIPLE_SUBPROPERTY = 103;			// Old value is 3,  We cluster SC SP EC EP, Since OWLEquivalenceSCSPMapper require input from
	public static final int SCHEMA_TRIPLE_MEMBER_SUBPROPERTY = 23;		// Old value is 20 which conflicts with TRANSITIVE_TRIPLE = 20 !!!!
	public static final int SCHEMA_TRIPLE_SUBCLASS = 104;				// Old value is 4, We cluster SC SP EC EP, Since OWLEquivalenceSCSPMapper require input from
	public static final int SCHEMA_TRIPLE_RESOURCE_SUBCLASS = 21;
	public static final int SCHEMA_TRIPLE_LITERAL_SUBCLASS = 22;
	
	//USED FOR OWL REASONER
	public static final int SCHEMA_TRIPLE_FUNCTIONAL_PROPERTY = 6;
	public static final int SCHEMA_TRIPLE_INVERSE_FUNCTIONAL_PROPERTY = 7;
	public static final int SCHEMA_TRIPLE_SYMMETRIC_PROPERTY = 8;
	public static final int SCHEMA_TRIPLE_TRANSITIVE_PROPERTY = 9;
	public static final int DATA_TRIPLE_SAME_AS = 10;
	public static final int SCHEMA_TRIPLE_INVERSE_OF = 11;
	public static final int DATA_TRIPLE_CLASS_TYPE = 12;
	public static final int DATA_TRIPLE_PROPERTY_TYPE = 13; 		
	public static final int SCHEMA_TRIPLE_EQUIVALENT_CLASS = 114;	// Old 14, We cluster SC SP EC EP, Since OWLEquivalenceSCSPMapper require input from
	public static final int SCHEMA_TRIPLE_EQUIVALENT_PROPERTY = 115;// Old 15, We cluster SC SP EC EP, Since OWLEquivalenceSCSPMapper require input from
	public static final int DATA_TRIPLE_HAS_VALUE = 16;
	public static final int SCHEMA_TRIPLE_ON_PROPERTY = 17;
	public static final int SCHEMA_TRIPLE_SOME_VALUES_FROM = 18;
	public static final int SCHEMA_TRIPLE_ALL_VALUES_FROM = 19;
	public static final int TRANSITIVE_TRIPLE = 20;
	// Added by Wugang 20150108, same as synonyms table, related to 5,6,7,9,10.11
	public static final int SYNONYMS_TABLE = 56791011;	 

	//FILE PREFIXES
	public static final String DIR_PREFIX = "dir-";
	public static final String OWL_PREFIX = "owl-";
	
	//FILE SUFFIXES
	public static final String FILE_SUFF_OTHER_DATA = "-other-data";
	public static final String FILE_SUFF_RDF_TYPE = "-type-data";
	public static final String FILE_SUFF_OWL_SYMMETRIC_TYPE = "-symmetric-property-type-data";
	public static final String FILE_SUFF_OWL_TRANSITIVE_TYPE = "-transitive-property-type-data";
	public static final String FILE_SUFF_OWL_CLASS_TYPE = "-rdfs-class-type-data";
	public static final String FILE_SUFF_OWL_PROPERTY_TYPE = "-owl-property-type-data";
	public static final String FILE_SUFF_OWL_FUNCTIONAL_PROPERTY_TYPE = "-functional-property-type-data";
	public static final String FILE_SUFF_OWL_INV_FUNCTIONAL_PROPERTY_TYPE = "-inverse-functional-property-type-data";
	
	public static final String FILE_SUFF_RDFS_SUBCLASS = "-subclas-schema";
	public static final String FILE_SUFF_RDFS_RESOURCE_SUBCLASS = "-resource-subclas-schema";
	public static final String FILE_SUFF_RDFS_LITERAL_SUBCLASS = "-literal-subclas-schema";
	public static final String FILE_SUFF_RDFS_SUBPROP = "-subprop-schema";
	public static final String FILE_SUFF_RDFS_DOMAIN = "-domain-schema";
	public static final String FILE_SUFF_RDFS_RANGE = "-range-schema";
	public static final String FILE_SUFF_RDFS_MEMBER_SUBPROP = "-member-subprop-schema";
	public static final String FILE_SUFF_OWL_SAME_AS = "-same-as-data";
	public static final String FILE_SUFF_OWL_INVERSE_OF = "-inverse-of-schema";
	public static final String FILE_SUFF_OWL_EQUIVALENT_CLASS = "-equivalent-class-schema";
	public static final String FILE_SUFF_OWL_EQUIVALENT_PROPERTY = "-equivalent-property-schema";
	public static final String FILE_SUFF_OWL_HAS_VALUE = "-has-value-data";
	public static final String FILE_SUFF_OWL_ON_PROPERTY = "-on-property-schema";
	public static final String FILE_SUFF_OWL_SOME_VALUES = "-some-values-schema";
	public static final String FILE_SUFF_OWL_ALL_VALUES = "-all-values-schema";

	private TriplesUtils() {
		preloadedURIs = new HashMap<String, Long>();		
		preloadedURIs.put(S_RDF_TYPE,RDF_TYPE);
		preloadedURIs.put(S_RDF_PROPERTY,RDF_PROPERTY);
		preloadedURIs.put(S_RDFS_RANGE,RDFS_RANGE);
		preloadedURIs.put(S_RDFS_DOMAIN,RDFS_DOMAIN);
		preloadedURIs.put(S_RDFS_SUBPROPERTY,RDFS_SUBPROPERTY);
		preloadedURIs.put(S_RDFS_SUBCLASS,RDFS_SUBCLASS);
		preloadedURIs.put(S_RDFS_MEMBER,RDFS_MEMBER);
		preloadedURIs.put(S_RDFS_LITERAL,RDFS_LITERAL);
		preloadedURIs.put(S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY,RDFS_CONTAINER_MEMBERSHIP_PROPERTY);
		preloadedURIs.put(S_RDFS_DATATYPE,RDFS_DATATYPE);
		preloadedURIs.put(S_RDFS_CLASS,RDFS_CLASS);
		preloadedURIs.put(S_RDFS_RESOURCE,RDFS_RESOURCE);
		preloadedURIs.put(S_OWL_CLASS,OWL_CLASS);
		preloadedURIs.put(S_OWL_FUNCTIONAL_PROPERTY,OWL_FUNCTIONAL_PROPERTY);
		preloadedURIs.put(S_OWL_INVERSE_FUNCTIONAL_PROPERTY,OWL_INVERSE_FUNCTIONAL_PROPERTY);
		preloadedURIs.put(S_OWL_SYMMETRIC_PROPERTY,OWL_SYMMETRIC_PROPERTY);
		preloadedURIs.put(S_OWL_TRANSITIVE_PROPERTY,OWL_TRANSITIVE_PROPERTY);
		preloadedURIs.put(S_OWL_SAME_AS,OWL_SAME_AS);
		preloadedURIs.put(S_OWL_INVERSE_OF,OWL_INVERSE_OF);
		preloadedURIs.put(S_OWL_EQUIVALENT_CLASS,OWL_EQUIVALENT_CLASS);
		preloadedURIs.put(S_OWL_EQUIVALENT_PROPERTY,OWL_EQUIVALENT_PROPERTY);
		preloadedURIs.put(S_OWL_HAS_VALUE,OWL_HAS_VALUE);
		preloadedURIs.put(S_OWL_ON_PROPERTY,OWL_ON_PROPERTY);
		preloadedURIs.put(S_OWL_SOME_VALUES_FROM,OWL_SOME_VALUES_FROM);
		preloadedURIs.put(S_OWL_ALL_VALUES_FROM,OWL_ALL_VALUES_FROM);
		log.info("cache URIs size: " + preloadedURIs.size());
		
		// Added by WuGang 2010-12-1，反向 
		preloadedResources = new HashMap<Long, String>();
		preloadedResources.put(RDF_TYPE,S_RDF_TYPE);
		preloadedResources.put(RDF_PROPERTY,S_RDF_PROPERTY);
		preloadedResources.put(RDFS_RANGE,S_RDFS_RANGE);
		preloadedResources.put(RDFS_DOMAIN,S_RDFS_DOMAIN);
		preloadedResources.put(RDFS_SUBPROPERTY,S_RDFS_SUBPROPERTY);
		preloadedResources.put(RDFS_SUBCLASS,S_RDFS_SUBCLASS);
		preloadedResources.put(RDFS_MEMBER,S_RDFS_MEMBER);
		preloadedResources.put(RDFS_LITERAL,S_RDFS_LITERAL);
		preloadedResources.put(RDFS_CONTAINER_MEMBERSHIP_PROPERTY,S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY);
		preloadedResources.put(RDFS_DATATYPE,S_RDFS_DATATYPE);
		preloadedResources.put(RDFS_CLASS,S_RDFS_CLASS);
		preloadedResources.put(RDFS_RESOURCE,S_RDFS_RESOURCE);
		preloadedResources.put(OWL_CLASS,S_OWL_CLASS);
		preloadedResources.put(OWL_FUNCTIONAL_PROPERTY,S_OWL_FUNCTIONAL_PROPERTY);
		preloadedResources.put(OWL_INVERSE_FUNCTIONAL_PROPERTY,S_OWL_INVERSE_FUNCTIONAL_PROPERTY);
		preloadedResources.put(OWL_SYMMETRIC_PROPERTY,S_OWL_SYMMETRIC_PROPERTY);
		preloadedResources.put(OWL_TRANSITIVE_PROPERTY,S_OWL_TRANSITIVE_PROPERTY);
		preloadedResources.put(OWL_SAME_AS,S_OWL_SAME_AS);
		preloadedResources.put(OWL_INVERSE_OF,S_OWL_INVERSE_OF);
		preloadedResources.put(OWL_EQUIVALENT_CLASS,S_OWL_EQUIVALENT_CLASS);
		preloadedResources.put(OWL_EQUIVALENT_PROPERTY,S_OWL_EQUIVALENT_PROPERTY);
		preloadedResources.put(OWL_HAS_VALUE,S_OWL_HAS_VALUE);
		preloadedResources.put(OWL_ON_PROPERTY,S_OWL_ON_PROPERTY);
		preloadedResources.put(OWL_SOME_VALUES_FROM,S_OWL_SOME_VALUES_FROM);
		preloadedResources.put(OWL_ALL_VALUES_FROM,S_OWL_ALL_VALUES_FROM);
	}
	
	public static TriplesUtils getInstance() {
		if (instance == null) {
			instance = new TriplesUtils();
		}
		return instance;
	}

	public Map<String,Long> getPreloadedURIs() {
		return preloadedURIs;
	}
	
	public Map<Long, String> getInvertedPreloadedURIs(){
		return preloadedResources;
	}

	public static String getFileExtensionByTripleType(TripleSource source, Triple triple, String name) {
		int tripleType = getTripleType(source, triple.getSubject(), triple.getPredicate(), triple.getObject());
		
		if (tripleType == TRANSITIVE_TRIPLE) {
			return "dir-transitivity-base/dir-level-0/" + name;
		}
		
		if (tripleType == SCHEMA_TRIPLE_DOMAIN_PROPERTY) {
			return name + FILE_SUFF_RDFS_DOMAIN;
		}
		
		if (tripleType == SCHEMA_TRIPLE_RANGE_PROPERTY) {
			return name + FILE_SUFF_RDFS_RANGE;
		}
		
		if (tripleType == SCHEMA_TRIPLE_SUBPROPERTY) {
			return name + FILE_SUFF_RDFS_SUBPROP;
		}
		
		if (tripleType == SCHEMA_TRIPLE_MEMBER_SUBPROPERTY) {
			return name + FILE_SUFF_RDFS_MEMBER_SUBPROP;
		}
		
		if (tripleType == SCHEMA_TRIPLE_SUBCLASS) {
			return name + FILE_SUFF_RDFS_SUBCLASS;
		}
		
		if (tripleType == SCHEMA_TRIPLE_RESOURCE_SUBCLASS) {
			return name + FILE_SUFF_RDFS_RESOURCE_SUBCLASS;
		}
		
		if (tripleType == SCHEMA_TRIPLE_LITERAL_SUBCLASS) {
			return name + FILE_SUFF_RDFS_LITERAL_SUBCLASS;
		}
		
		if (tripleType == DATA_TRIPLE_TYPE) {
			return name + FILE_SUFF_RDF_TYPE;
		}
		
		//OWL SUBSETS
		if (tripleType == DATA_TRIPLE_CLASS_TYPE) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_CLASS_TYPE;
		}
		
		if (tripleType == DATA_TRIPLE_PROPERTY_TYPE) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_PROPERTY_TYPE;
		}
		
		if (tripleType == SCHEMA_TRIPLE_FUNCTIONAL_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_FUNCTIONAL_PROPERTY_TYPE;
		}
		
		if (tripleType == SCHEMA_TRIPLE_INVERSE_FUNCTIONAL_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_INV_FUNCTIONAL_PROPERTY_TYPE;
		}
		
		if (tripleType == SCHEMA_TRIPLE_SYMMETRIC_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_SYMMETRIC_TYPE;
		}
		
		if (tripleType == SCHEMA_TRIPLE_TRANSITIVE_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_TRANSITIVE_TYPE;
		}
		
		if (tripleType == DATA_TRIPLE_SAME_AS) {
			return "dir-synonymstable/" + TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_SAME_AS;
		}
		
		if (tripleType == SCHEMA_TRIPLE_INVERSE_OF) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_INVERSE_OF;
		}

		if (tripleType == SCHEMA_TRIPLE_EQUIVALENT_CLASS) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_EQUIVALENT_CLASS;
		}

		if (tripleType == SCHEMA_TRIPLE_EQUIVALENT_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_EQUIVALENT_PROPERTY;
		}

		if (tripleType == DATA_TRIPLE_HAS_VALUE) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_HAS_VALUE;
		}

		if (tripleType == SCHEMA_TRIPLE_ON_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_ON_PROPERTY;
		}

		if (tripleType == SCHEMA_TRIPLE_SOME_VALUES_FROM) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_SOME_VALUES;
		}

		if (tripleType == SCHEMA_TRIPLE_ALL_VALUES_FROM) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_ALL_VALUES;
		}
		
		return name + TriplesUtils.FILE_SUFF_OTHER_DATA;
	}

	public static int getTripleType(TripleSource source, long subject, long predicate, long object) {
		
		if (source.getDerivation() == TripleSource.TRANSITIVE_ENABLED 
				|| source.getDerivation() == TripleSource.TRANSITIVE_DISABLED) {
			return TRANSITIVE_TRIPLE;
		}
		
		byte newKey = DATA_TRIPLE;
		if (predicate == RDFS_RANGE) {
			newKey = SCHEMA_TRIPLE_RANGE_PROPERTY;
		} else if (predicate == RDFS_DOMAIN) {
			newKey = SCHEMA_TRIPLE_DOMAIN_PROPERTY;
		} else if (predicate == RDFS_SUBPROPERTY) {
			if (object == RDFS_MEMBER)
				newKey = SCHEMA_TRIPLE_MEMBER_SUBPROPERTY;
			else
				newKey = SCHEMA_TRIPLE_SUBPROPERTY;
		} else if (predicate == RDFS_SUBCLASS) {
			if (object == RDFS_RESOURCE)
				newKey = SCHEMA_TRIPLE_RESOURCE_SUBCLASS;
			else if (object == RDFS_LITERAL)
				newKey = SCHEMA_TRIPLE_LITERAL_SUBCLASS;
			else
				newKey = SCHEMA_TRIPLE_SUBCLASS;
		} else if (predicate == RDF_TYPE) {
			if (object == OWL_CLASS)
				newKey = DATA_TRIPLE_CLASS_TYPE;
			else if (object == RDF_PROPERTY)
				newKey = DATA_TRIPLE_PROPERTY_TYPE;
			else if (object == OWL_FUNCTIONAL_PROPERTY)
				newKey = SCHEMA_TRIPLE_FUNCTIONAL_PROPERTY;
			else if (object == OWL_INVERSE_FUNCTIONAL_PROPERTY)
				newKey = SCHEMA_TRIPLE_INVERSE_FUNCTIONAL_PROPERTY;
			else if (object == OWL_SYMMETRIC_PROPERTY)
				newKey = SCHEMA_TRIPLE_SYMMETRIC_PROPERTY;
			else if (object == OWL_TRANSITIVE_PROPERTY)
				newKey = SCHEMA_TRIPLE_TRANSITIVE_PROPERTY;
			else
				newKey = DATA_TRIPLE_TYPE;
		} else if (predicate == OWL_SAME_AS) {
			newKey = DATA_TRIPLE_SAME_AS;
		} else if (predicate == OWL_INVERSE_OF) {
			newKey = SCHEMA_TRIPLE_INVERSE_OF;
		} else if (predicate == OWL_EQUIVALENT_CLASS) {
			newKey = SCHEMA_TRIPLE_EQUIVALENT_CLASS;
		} else if (predicate == OWL_EQUIVALENT_PROPERTY) {
			newKey = SCHEMA_TRIPLE_EQUIVALENT_PROPERTY;
		} else if (predicate == OWL_HAS_VALUE) {
			newKey = DATA_TRIPLE_HAS_VALUE;
		} else if (predicate == OWL_ON_PROPERTY) {
			newKey = SCHEMA_TRIPLE_ON_PROPERTY;
		} else if (predicate == OWL_SOME_VALUES_FROM) {
			newKey = SCHEMA_TRIPLE_SOME_VALUES_FROM;
		} else if (predicate == OWL_ALL_VALUES_FROM) {
//			log.info("-->This is a OWL_ALL_VALUES_FROM type triple.");
			newKey = SCHEMA_TRIPLE_ALL_VALUES_FROM;
		}

		return newKey;
	}
	
	public static String[] parseTriple(String triple, String fileId) throws Exception {
		String[] values = new String[3];
		
		//Parse subject
		int subjectlength = 0;
		if (triple.startsWith("<")) {
			values[0] = triple.substring(0,triple.indexOf('>') + 1);
			subjectlength = values[0].length();
		} else { //Is a bnode
			// Modified by WuGang, change it to that of object
			//values[0] = triple.substring(0,triple.indexOf(' '));
			values[0] = "_:" + fileId + triple.substring(1,triple.indexOf(' '));
			subjectlength = triple.substring(0,triple.indexOf(' ')).length();
		}
		
//		System.out.println("-->Subject："+values[0]);
		
		triple = triple.substring(subjectlength + 1);
		//Parse predicate. It can be only a URI
		values[1] = triple.substring(0,triple.indexOf('>') + 1);
		
//		System.out.println("-->Predicate："+values[1]);
//		if (values[1].equals(S_OWL_ALL_VALUES_FROM))
//			System.out.println("I found a S_OWL_ALL_VALUES_FROM");

		//Parse object
		triple = triple.substring(values[1].length() + 1);
		if (triple.startsWith("<")) { //URI
			values[2] = triple.substring(0,triple.indexOf('>') + 1);
		} else if (triple.charAt(0) == '"') { //Literal
			values[2] = triple.substring(0,triple.substring(1).indexOf('"') + 2);
			triple = triple.substring(values[2].length(), triple.length());
			values[2] += triple.substring(0,triple.indexOf(' '));
		} else { //Bnode
			values[2] = "_:" + fileId + triple.substring(1,triple.indexOf(' '));			
		}
		
//		System.out.println("-->Object："+values[2]);

		return values;
	}
	
	public static void createTripleIndex(byte[] bytes, Triple value, String index) {
		if (index.equalsIgnoreCase("spo")) {
			NumberUtils.encodeLong(bytes, 0, value.getSubject());
			NumberUtils.encodeLong(bytes, 8, value.getPredicate());
			NumberUtils.encodeLong(bytes, 16, value.getObject());
		} else if (index.equalsIgnoreCase("pos")) {
			NumberUtils.encodeLong(bytes, 0, value.getPredicate());
			NumberUtils.encodeLong(bytes, 8, value.getObject());
			NumberUtils.encodeLong(bytes, 16, value.getSubject());
		} else {
			NumberUtils.encodeLong(bytes, 0, value.getObject());
			NumberUtils.encodeLong(bytes, 8, value.getSubject());
			NumberUtils.encodeLong(bytes, 16, value.getPredicate());				
		}
	}
}
