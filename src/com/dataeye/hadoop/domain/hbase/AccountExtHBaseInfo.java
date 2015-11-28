package com.dataeye.hadoop.domain.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class AccountExtHBaseInfo {
	
	public static final byte[] account_ext_info = Bytes.toBytes("account_ext_info");
	public static final byte[] cf_gs = Bytes.toBytes("gs");
	public static final byte[] cf_uid = Bytes.toBytes("uid");
	public static final byte[] cf_role = Bytes.toBytes("role");
}
