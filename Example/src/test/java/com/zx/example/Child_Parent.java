package com.zx.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Child_Parent implements WritableComparable<Child_Parent> {

	private String child;
	private String parent;

	public Child_Parent() {
	}

	public Child_Parent(String child, String parent) {
		this.child = child;
		this.parent = parent;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(child);
		out.writeUTF(parent);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.child = in.readUTF();
		this.parent = in.readUTF();
	}

	@Override
	public int compareTo(Child_Parent o) {
		if (this.parent.equals(o.child) || this.child.equals(o.parent))
			return 0;
		return -1;
	}

	@Override
	public String toString() {
		return "child = " + this.child + ",parent = " + this.parent;
	}

}
