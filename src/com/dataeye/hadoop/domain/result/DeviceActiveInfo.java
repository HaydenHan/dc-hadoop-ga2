package com.dataeye.hadoop.domain.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;
import com.dataeye.hadoop.domain.kv.MVUIDActive;
import com.dataeye.hadoop.util.StringUtil;

public class DeviceActiveInfo extends MVUIDActive {

	private int isActAndReg;
	private String accIdSet = MRConstants.STR_EMPTY_SET_JSON;

	public int getIsActAndReg() {
		return isActAndReg;
	}

	public void setIsActAndReg(int isActAndReg) {
		this.isActAndReg = isActAndReg;
	}

	public String getAccIdSet() {
		return accIdSet;
	}

	public void setAccIdSet(String accIdSet) {
		this.accIdSet = accIdSet;
	}

	public DeviceActiveInfo() {
	}

	public DeviceActiveInfo(MVUIDActive active) {
		super(active);
	}

	public DeviceActiveInfo(String[] fields) {
		super(fields);
		this.isActAndReg = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.accIdSet = fields[fieldsIndex++];
	}

	public DeviceActiveInfo(Header header, Event event) {
		super(header, event);
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(isActAndReg);
		out.writeUTF(accIdSet);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isActAndReg = in.readInt();
		this.accIdSet = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, isActAndReg);
		super.writeUTF8(out, accIdSet);
	}

}
