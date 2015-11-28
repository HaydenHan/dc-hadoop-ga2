package com.dataeye.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

	private static Connection hConnection = null;

	public static void init(Configuration conf) {
		if (conf.getBoolean("dc.ishbase.job", false)) {
			String zk_quorum = conf.get("hbase.zookeeper.quorum");
			String zk_port = conf.get("hbase.zookeeper.property.clientPort");
			System.out.println("zk_quorum:" + zk_quorum);
			System.out.println("zk_port:" + zk_port);
			if (StringUtil.isEmpty(zk_quorum) || StringUtil.isEmpty(zk_port)) {
				return;
			}
			try {
				init(zk_quorum, zk_port);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static synchronized void init(String zk_quorum, String zk_port) throws Exception {
		if (null != hConnection) {
			return;
		}
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", zk_quorum);
		configuration.set("hbase.zookeeper.property.clientPort", zk_port);
		hConnection = ConnectionFactory.createConnection(configuration);
	}

	public static Connection getConnection() throws ZooKeeperConnectionException {
		return hConnection;
	}

	public static Table getTable(String table) throws IOException {
		return getTable(Bytes.toBytes(table));
	}

	public static Table getTable(byte[] table) throws IOException {
		TableName tableNameObj = TableName.valueOf(table);
		return hConnection.getTable(tableNameObj);
	}

	public static Result doGet(byte[] table, Get get) throws IOException {
		return getTable(table).get(get);
	}

	public static void doPut(byte[] table, Put put) throws IOException {
		getTable(table).put(put);
	}

	public static boolean checkExist(byte[] table, Get get) throws IOException {
		return getTable(table).exists(get);
	}

	public static void close() throws IOException {
		hConnection.close();
	}
}
