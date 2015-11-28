package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.DEEventIds;
import com.dataeye.hadoop.domain.common.DEHeaderWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;

public class MVUIDAccountId extends DEHeaderWritable {

	private String accountId;

	public String getAccountId() {
		return accountId;
	}

	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	public MVUIDAccountId() {
	}

	public MVUIDAccountId(Header header, Event event) {
		this.accountId = event.getLabelMap().get(DEEventIds.DES_AccountId);
	}
	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(accountId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.accountId = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsUTF8Bytes(out, accountId);
	}

}
