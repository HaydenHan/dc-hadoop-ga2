package com.dataeye.hadoop.common.dewritable;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.WritableComparable;

public abstract class DEWritable implements WritableComparable<DEWritable>{
	
	public static final String ENCODER_CHARSET = "UTF-8";
	public static final String FIELDS_SEPARATOR = "\t";
	public static final byte[] SEPARATOR_IN_BYTES = intSep2Bytes();
	
	private static byte[] intSep2Bytes(){
		try {
			return FIELDS_SEPARATOR.getBytes(ENCODER_CHARSET);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	public abstract void writeAsString(DataOutput out) throws IOException;
	
	public void writeSeparator(DataOutput out) throws UnsupportedEncodingException, IOException{
		out.write(SEPARATOR_IN_BYTES);
	}
	
	public void writeAsUTF8Bytes(DataOutput out, Object obj) throws UnsupportedEncodingException, IOException{
		out.write(obj.toString().getBytes(ENCODER_CHARSET));
	}
	
	public void writeUTF8(DataOutput out, Object obj) throws UnsupportedEncodingException, IOException{
		out.write(obj.toString().getBytes(ENCODER_CHARSET));
	}
	
	public void writeUTF8WithSep(DataOutput out, Object obj) throws UnsupportedEncodingException, IOException{
		out.write(obj.toString().getBytes(ENCODER_CHARSET));
		writeSeparator(out);
	}
}
