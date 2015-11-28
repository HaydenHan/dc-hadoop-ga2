package com.dataeye.hadoop.domain.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class AccountHistHBaseInfo extends HBaseBasicInfo {

	public static final byte[] account_history_info = Bytes.toBytes("account_history_info");
	public static final byte[] cf_static = Bytes.toBytes("stac");
	public static final byte[] cf_dynamic = Bytes.toBytes("dymc");

	public static final byte[] d_q_gs = Bytes.toBytes("gs");
	public static final byte[] d_q_fgs = Bytes.toBytes("fgs");
	public static final byte[] d_q_account_id = Bytes.toBytes("accId");
	public static final byte[] d_q_account_type = Bytes.toBytes("accType");
	public static final byte[] d_q_gender = Bytes.toBytes("gender");
	public static final byte[] d_q_age = Bytes.toBytes("age");
	public static final byte[] d_q_createTime = Bytes.toBytes("ct");
	public static final byte[] d_q_firstLoginDate = Bytes.toBytes("fld");
	public static final byte[] d_q_firstLoginTime = Bytes.toBytes("flt");
	public static final byte[] d_q_lastLoginDate = Bytes.toBytes("lld");
	public static final byte[] d_q_lastLoginTime = Bytes.toBytes("llt");
	public static final byte[] d_q_totalOnlineDays = Bytes.toBytes("tods");
	public static final byte[] d_q_totalLoginTimes = Bytes.toBytes("tlts");
	public static final byte[] d_q_totalOnlineTime = Bytes.toBytes("tot");
	public static final byte[] d_q_firstPayDate = Bytes.toBytes("fpd");
	public static final byte[] d_q_firstPayTime = Bytes.toBytes("fpt");
	public static final byte[] d_q_lastPayDate = Bytes.toBytes("lpd");
	public static final byte[] d_q_lastPayTime = Bytes.toBytes("lpt");
	public static final byte[] d_q_lastPayAmount = Bytes.toBytes("lpa");
	public static final byte[] d_q_totalPayAmount = Bytes.toBytes("tpa");
	public static final byte[] d_q_totalPayTimes = Bytes.toBytes("tpts");
	public static final byte[] d_q_olTrack = Bytes.toBytes("olTrack");
	public static final byte[] d_q_payTrack = Bytes.toBytes("payTrack");
}
