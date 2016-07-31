package cn.edu.neu.mitt.mrj.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FileUtils {
	
	public static final PathFilter FILTER_ONLY_HIDDEN = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith("."); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_SUBPROP_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX) || name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBPROP)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_SUBCLASS_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
//			System.out.print("测试FILTER_ONLY_SUBCLASS_SCHEMA路径-"+p+"-结果为-");
//			System.out.println(!name.startsWith("_") && !name.startsWith(".") && 
//					(name.startsWith(TriplesUtils.DIR_PREFIX) || name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS)));

			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX) || name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_TYPE_SUBCLASS = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
//			System.out.print("测试FILTER_ONLY_TYPE_SUBCLASS路径-"+p+"-结果为-");
//			System.out.println(!name.startsWith("_") && !name.startsWith(".") && 
//					(name.startsWith(TriplesUtils.DIR_PREFIX) || 
//							name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS) || 
//							name.endsWith(TriplesUtils.FILE_SUFF_RDF_TYPE)));

			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX) || 
					name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS) || 
					name.endsWith(TriplesUtils.FILE_SUFF_RDF_TYPE)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_DOMAIN_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX) || name.endsWith(TriplesUtils.FILE_SUFF_RDFS_DOMAIN)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_RANGE_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX) || name.endsWith(TriplesUtils.FILE_SUFF_RDFS_RANGE)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_MEMBER_SUBPROP_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX) || name.endsWith(TriplesUtils.FILE_SUFF_RDFS_MEMBER_SUBPROP)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_RESOURCE_SUBCLAS_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") &&
			(name.startsWith(TriplesUtils.DIR_PREFIX) 
					|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_RESOURCE_SUBCLASS));
		}
	};
	
	public static final PathFilter FILTER_ONLY_LITERAL_SUBCLAS_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") &&
			(name.startsWith(TriplesUtils.DIR_PREFIX) 
					|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_LITERAL_SUBCLASS));
		}
	};
	
	public static final PathFilter FILTER_ONLY_OTHER_SUBCLASS_SUBPROP = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") &&
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS)
					|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBPROP)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OTHER_DATA)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_FUNCTIONAL_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_FUNCTIONAL_PROPERTY_TYPE)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_INVERSE_FUNCTIONAL_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_INV_FUNCTIONAL_PROPERTY_TYPE)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_SYMMETRIC_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_SYMMETRIC_TYPE)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_TRANSITIVE_SCHEMA = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_TRANSITIVE_TYPE)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_INVERSE_OF = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_INVERSE_OF)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_SAMEAS = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_SAME_AS)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_SUBCLASS_SUBPROP_EQ_CLASSPROP = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") &&
			(name.startsWith(TriplesUtils.DIR_PREFIX) 
					|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS) 
					|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBPROP)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_EQUIVALENT_CLASS)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_EQUIVALENT_PROPERTY)); 
		}
	};
	
	// Added by WuGang
	public static final PathFilter FILTER_ONLY_EQ_CLASS = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") &&
			(name.startsWith(TriplesUtils.DIR_PREFIX) 
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_EQUIVALENT_CLASS)); 
		}
	};

	// Added by WuGang
	public static final PathFilter FILTER_ONLY_EQ_PROP = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") &&
			(name.startsWith(TriplesUtils.DIR_PREFIX) 
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_EQUIVALENT_PROPERTY)); 
		}
	};

	
	public static final PathFilter FILTER_ONLY_OWL_ON_PROPERTY = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_ON_PROPERTY)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_SOME_VALUES = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_SOME_VALUES)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_ALL_VALUES = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_ALL_VALUES)); 
		}
	};
	
	public static final PathFilter FILTER_ONLY_OWL_ON_PROPERTY_HAS_VALUE = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_ON_PROPERTY)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_HAS_VALUE)); 
		}
	};
	
	// Added by WuGang.
	public static final PathFilter FILTER_ONLY_OWL_HAS_VALUE = new PathFilter() {		
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && 
			(name.startsWith(TriplesUtils.DIR_PREFIX)
					|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_HAS_VALUE)); 
		}
	};

}
