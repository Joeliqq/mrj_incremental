package cn.edu.neu.mitt.mrj.data;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import cn.edu.neu.mitt.mrj.utils.HashFunctions;

public class Triple implements WritableComparable<Triple>, Serializable {

	private static final long serialVersionUID = 7578125506159351415L;

	private long subject = 0;
	private long predicate = 0;
	private long object = 0;
	private long type = 0;
	private long rsubject = 0;
	private long rpredicate = 0;
	private long robject = 0;
	private boolean isObjectLiteral = false;

	public Triple(long s, long p, long o, boolean literal) {
		this.subject = s;
		this.predicate = p;
		this.object = o;
		this.isObjectLiteral = literal;
	}
	
	public Triple (Triple triple){
		this.subject = triple.subject;
		this.predicate = triple.predicate;
		this.object = triple.object;
		this.type = triple.type;
		this.rsubject = triple.rsubject;
		this.rpredicate = triple.rpredicate;
		this.robject = triple.robject;
		this.isObjectLiteral = triple.isObjectLiteral;
	}

	public Triple() { }

	@Override
	public void readFields(DataInput in) throws IOException {
		subject = in.readLong();
		predicate = in.readLong();
		object = in.readLong();
		byte flags = in.readByte();
		isObjectLiteral = (flags >> 1) == 1;
		type = in.readLong();
		rsubject = in.readLong();
		rpredicate = in.readLong();
		robject = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(subject);
		out.writeLong(predicate);
		out.writeLong(object);
		byte flag = 0;
		if (isObjectLiteral)
			flag = (byte) (flag | 0x2);
		out.writeByte(flag);
		out.writeLong(type);
		out.writeLong(rsubject);
		out.writeLong(rpredicate);
		out.writeLong(robject);
	}

	public static Triple read(DataInput in) throws IOException {
        Triple w = new Triple();
        w.readFields(in);
        return w;
      }
	
	public String toString() {
		return "(" + subject + " " + predicate + " " + object + ")-" + type + "->(" + rsubject + " " + rpredicate + " " + robject + ")";
	}


	public int hashCode() {
		return HashFunctions.DJBHashInt(toString());
	}

	@Override
	public int compareTo(Triple o) {
		if (subject == o.subject && predicate == o.predicate && object == o.object && 
				type == o.type && rsubject == o.rsubject && rpredicate == o.rpredicate && robject == o.robject)
			return 0;
		else {
			if (subject > o.subject) {
				return 1;
			} else { 
				if (subject == o.subject) { // Check the predicate
					if (predicate > o.predicate) {
						return 1;
				    } else {
				    	if (predicate == o.predicate) { //Check the object
				    		if (object > o.object) {
				    			return 1;
				    		} else {
				    			if (object == o.object) { // check the type
				    				if (type > o.type){
				    					return 1;
				    				}else{
				    					if (type == o.type){
				    						if (rsubject > o.rsubject){
				    							return 1;
				    						}else{
				    							if (rsubject == o.rsubject){
				    								if (rpredicate > o.rpredicate){
				    									return 1;
				    								}else{
				    									if (rpredicate == o.rpredicate){
				    										if (robject > o.robject)
				    											return 1;
				    										else
				    											return -1;
				    									}
				    									return -1;
				    								}
				    							}
				    							return -1;
				    						}
				    					}
				    					return -1;
				    				}
				    			}
				    			return -1;
					    	}
					    }
					    return -1;
				    }
				}
				return -1;
			}
		}
			
	}
	
	public boolean equals(Object triple) {
		if (compareTo((Triple)triple) == 0)
			return true;
		else
			return false;
	}

	public long getSubject() {
		return subject;
	}

	public void setSubject(long subject) {
		this.subject = subject;
	}

	public long getPredicate() {
		return predicate;
	}

	public void setPredicate(long predicate) {
		this.predicate = predicate;
	}

	public long getObject() {
		return object;
	}

	public void setObject(long object) {
		this.object = object;
	}

	public boolean isObjectLiteral() {
		return isObjectLiteral;
	}

	public void setObjectLiteral(boolean isObjectLiteral) {
		this.isObjectLiteral = isObjectLiteral;
	}
	
	
	
	public long getType() {
		return type;
	}

	public void setType(long type) {
		this.type = type;
	}

	public long getRsubject() {
		return rsubject;
	}

	public void setRsubject(long rsubject) {
		this.rsubject = rsubject;
	}

	public long getRpredicate() {
		return rpredicate;
	}

	public void setRpredicate(long rpredicate) {
		this.rpredicate = rpredicate;
	}

	public long getRobject() {
		return robject;
	}

	public void setRobject(long robject) {
		this.robject = robject;
	}



	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(Triple.class);
		}
		
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    	return compareBytes(b1, s1, l1 - 1, b2, s2, l2 - 1);
	    }
	}
	
	static {
		WritableComparator.define(Triple.class, new Comparator());
	}
}
