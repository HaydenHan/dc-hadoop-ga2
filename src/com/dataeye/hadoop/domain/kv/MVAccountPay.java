package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;

public class MVAccountPay extends AccountBaseWritable {

	private int payTime;
	private float payAmount;
	private String payType;
	
	public int getPayTime() {
		return payTime;
	}

	public void setPayTime(int payTime) {
		this.payTime = payTime;
	}

	public float getPayAmount() {
		return payAmount;
	}

	public void setPayAmount(float payAmount) {
		this.payAmount = payAmount;
	}

	public String getPayType() {
		return payType;
	}

	public void setPayType(String payType) {
		this.payType = payType;
	}

	public MVAccountPay() {
	}

	public MVAccountPay(MVAccountPay accPay) {
		super(accPay);
		this.payTime = accPay.getPayTime();
		this.payAmount = accPay.getPayAmount();
		this.payType = accPay.getPayType();
	}
	
	public MVAccountPay(Header header, Event event) {
		super(header, event);
		this.payTime = event.getPayTime();
		this.payAmount = event.getPayAmount();
		this.payType = event.getPayType();
	}
	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(payTime);
		out.writeFloat(payAmount);
		out.writeUTF(payType);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.payTime = in.readInt();
		this.payAmount = in.readFloat();
		this.payType = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, payTime);
		super.writeUTF8WithSep(out, payAmount);
		super.writeUTF8(out, payType);
	}
}
