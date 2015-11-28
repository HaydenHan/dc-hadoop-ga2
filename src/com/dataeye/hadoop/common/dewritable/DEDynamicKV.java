package com.dataeye.hadoop.common.dewritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.io.WritableComparable;

public class DEDynamicKV implements WritableComparable<DEDynamicKV> {

	private static final Class<?>[] EMPTY_ARRAY = new Class[] {};

	private String className;
	private DEWritable deWritable;

	public DEWritable getDeWritable() {
		return deWritable;
	}

	public void setDeWritable(DEWritable deWritable) {
		this.deWritable = deWritable;
	}

	public DEDynamicKV() {
	}

	public DEDynamicKV(DEWritable deWritable) {
		this.deWritable = deWritable;
	}

	@Override
	public int compareTo(DEDynamicKV o) {
		String clsName1 = deWritable.getClass().getName();
		String clsName2 = o.getDeWritable().getClass().getName();
		if (!clsName1.equals(clsName2)) {
			return clsName1.compareTo(clsName2);
		}
		return deWritable.compareTo(o.getDeWritable());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// read actual class
		String curClsName = in.readUTF();
		if (!curClsName.equals(className)) {
			deWritable = initDCWritable(curClsName);
			className = curClsName;
		}
		deWritable.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// write actual class
		out.writeUTF(deWritable.getClass().getName());
		//
		deWritable.write(out);
	}

	private DEWritable initDCWritable(String className) {
		try {
			Class<DEWritable> theClass = (Class<DEWritable>) Class.forName(className);
			Constructor<DEWritable> meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
			meth.setAccessible(true);
			return meth.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		System.out.println(DEDynamicKV.class.getName());
	}
}
