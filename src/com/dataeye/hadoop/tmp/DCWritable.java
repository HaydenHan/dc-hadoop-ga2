package com.dataeye.hadoop.tmp;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public abstract class DCWritable {
	
	public static final String ENCODER_CHARSET = "UTF-8";
	public static final String FIELDS_SEPARATOR = "\t";
	public static final byte[] SEPARATOR_IN_BYTES = intSep2Bytes();
	
	private static byte[] intSep2Bytes(){
		try {
			return FIELDS_SEPARATOR.getBytes(ENCODER_CHARSET);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public abstract void writeAsString(DataOutput out) throws IOException;
	
	public void writeSeparator(DataOutput out) throws UnsupportedEncodingException, IOException{
		out.write(SEPARATOR_IN_BYTES);
	}
	
	/*public void writeStrAsUTF8Bytes(DataOutput out, String str) throws UnsupportedEncodingException, IOException{
		out.write(str.getBytes(ENCODER_CHARSET));
	}*/
	
	public void writeAsUTF8Bytes(DataOutput out, Object obj) throws UnsupportedEncodingException, IOException{
		out.write(obj.toString().getBytes(ENCODER_CHARSET));
	}
}
