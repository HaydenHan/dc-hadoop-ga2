package com.dataeye.hadoop.tmp.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.io.WritableComparable;

public class DCDynamicKV implements WritableComparable<DCDynamicKV> {

	private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
	
	private String className;
	private DCWritable dcWritable;

	public DCWritable getDcWritable() {
		return dcWritable;
	}

	public void setDcWritable(DCWritable dcWritable) {
		this.dcWritable = dcWritable;
	}

	public DCDynamicKV(){}

	public DCDynamicKV(DCWritable dcWritable) {
		this.dcWritable = dcWritable;
	}

	@Override
	public int compareTo(DCDynamicKV o) {
		String clsName1 = dcWritable.getClass().getName();
		String clsName2 = o.getDcWritable().getClass().getName();
		if(!clsName1.equals(clsName2)){
			System.out.println("class---" + dcWritable + " : " + o.getDcWritable());
			
			return clsName1.compareTo(clsName2);
		}
		return dcWritable.compareTo(o.getDcWritable());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//read actual class
		String curClsName = in.readUTF();
		if(!curClsName.equals(className)){
			dcWritable = initDCWritable(curClsName);
			className = curClsName;
		}
		
		System.out.println("this="+this + "\n this.dcWritable="+this.dcWritable 
				+"\n dcWritable="+dcWritable + "\n className=" + className + ", curClsName="+curClsName);
		dcWritable.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//write actual class
		out.writeUTF(dcWritable.getClass().getName());
		//
		dcWritable.write(out);
	}
	
	private DCWritable initDCWritable(String className){
		try {
			Class<DCWritable> theClass = (Class<DCWritable>)Class.forName(className);
			Constructor<DCWritable> meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
		    meth.setAccessible(true);
		    return meth.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String[] args){
		System.out.println(DCDynamicKV.class.getName());
	}
}
