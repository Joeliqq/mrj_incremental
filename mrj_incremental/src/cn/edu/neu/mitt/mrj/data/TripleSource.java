package cn.edu.neu.mitt.mrj.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.set_cql_version;
import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TripleSource implements WritableComparable<TripleSource> {

	public static final byte RDFS_DERIVED = 1;
	public static final byte OWL_DERIVED = 2;
	
	public static final byte TRANSITIVE_ENABLED = 3;
	public static final byte TRANSITIVE_DISABLED = 4;
	
	byte derivation = 0;
	int step = 0;
	int transitive_level = 0;
	int updatelabel = 0;		// ?

	@Override
	public void readFields(DataInput in) throws IOException {
		derivation = in.readByte();
		step = in.readInt();
		transitive_level = in.readInt();
		updatelabel = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(derivation);
		out.writeInt(step);
		out.writeInt(transitive_level);
		out.writeInt(updatelabel);
	}

	@Override
	//I know need this method
	public int compareTo(TripleSource arg0) {
		return 0;		
	}

	public boolean isTripleDerived() {
		return derivation != 0;
	}
	
	public int getStep() {
		return step;
	}
	
	public void setStep(int step) {
		this.step = step;
	}
	
	public int getTransitiveLevel() {
		return transitive_level;
	}
	
	public void setTransitiveLevel(int level) {
		this.transitive_level = level;
	}
	
	public Integer getLabel(){
		return updatelabel;
	}
	
	public void setLabel(Integer r){
		this.updatelabel = r;
	}
	
	public void setDerivation(byte ruleset) {
		derivation = ruleset;
	}
	
	public byte getDerivation() {
		return derivation;
	}
}
