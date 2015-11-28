//package com.dataeye.hadoop.util;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.util.Bytes;
//
//public class HbaseProxyClient {
//
//	/**
//	 * 插入一行记录
//	 */
//	public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) {
//		Connection connection = null;
//		Table table = null;
//		try {
//			connection = HbaseConnectionManager.getConnection();
//
//			TableName tableNameObj = TableName.valueOf(tableName);
//			table = connection.getTable(tableNameObj);
//			Put put = new Put(Bytes.toBytes(rowKey));
//			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
//			// old api
//			// put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
//			table.put(put);
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			HbaseConnectionManager.close(table, connection);
//		}
//	}
//
//	/**
//	 * 删除一行记录
//	 */
//	public static void delRecord(String tableName, String rowKey) throws IOException {
//		Connection connection = null;
//		Table table = null;
//		try {
//			connection = HbaseConnectionManager.getConnection();
//			TableName tableNameObj = TableName.valueOf(tableName);
//			table = connection.getTable(tableNameObj);
//			List<Delete> list = new ArrayList<Delete>();
//			Delete del = new Delete(rowKey.getBytes());
//			list.add(del);
//			table.delete(list);
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		} finally {
//			HbaseConnectionManager.close(table, connection);
//		}
//
//	}
//
//	/**
//	 * 查找一行记录
//	 */
//	public static Result getOneRecord(Connection connection, String tableName, String rowKey) throws IOException {
//		Table table = null;
//		try {
//			TableName tableNameObj = TableName.valueOf(tableName);
//			table = connection.getTable(tableNameObj);
//			Get get = new Get(Bytes.toBytes(rowKey));
//			Result rs = table.get(get);
//			return rs;
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		} finally {
//			HbaseConnectionManager.close(table, null);
//		}
//	}
//
//	/**
//	 * 查找一行记录
//	 */
//	public static Result getOneRecord(String tableName, String rowKey) throws IOException {
//
//		Connection connection = null;
//		Table table = null;
//		try {
//			connection = HbaseConnectionManager.getConnection();
//			TableName tableNameObj = TableName.valueOf(tableName);
//			table = connection.getTable(tableNameObj);
//			Get get = new Get(Bytes.toBytes(rowKey));
//			Result rs = table.get(get);
//			return rs;
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		} finally {
//			HbaseConnectionManager.close(table, connection);
//		}
//	}
//
//	/**
//	 * 查找某个 cf 的一行记录
//	 */
//	public static Result getCFOneRecord(String tableName, String rowKey, String columnFamily) throws IOException {
//
//		Connection connection = null;
//		Table table = null;
//		try {
//			connection = HbaseConnectionManager.getConnection();
//			TableName tableNameObj = TableName.valueOf(tableName);
//			table = connection.getTable(tableNameObj);
//			Get get = new Get(Bytes.toBytes(rowKey));
//			get.addFamily(Bytes.toBytes(columnFamily));
//			Result rs = table.get(get);
//			return rs;
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		} finally {
//			HbaseConnectionManager.close(table, connection);
//		}
//	}
//
//	public static Result getResultByTableNameGet(String tableName, Get get) throws IOException {
//		Connection connection = null;
//		Table table = null;
//		try {
//			connection = HbaseConnectionManager.getConnection();
//			TableName tableNameObj = TableName.valueOf(tableName);
//			table = connection.getTable(tableNameObj);
//			Result rs = table.get(get);
//			return rs;
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		} finally {
//			HbaseConnectionManager.close(table, connection);
//		}
//	}
//
//	public static void addRecordByTablePut(String tableName, Put put) {
//		Connection connection = null;
//		Table table = null;
//		try {
//			connection = HbaseConnectionManager.getConnection();
//
//			TableName tableNameObj = TableName.valueOf(tableName);
//			table = connection.getTable(tableNameObj);
//			table.put(put);
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			HbaseConnectionManager.close(table, connection);
//		}
//	}
//
//	/**
//	 * 插入一行记录
//	 */
//	public static boolean checkExist(String tableName, Get get) {
//		Connection connection = null;
//		Table table = null;
//		try {
//			connection = HbaseConnectionManager.getConnection();
//			TableName tableNameObj = TableName.valueOf(tableName);
//			table = connection.getTable(tableNameObj);
//			return table.exists(get);
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			HbaseConnectionManager.close(table, connection);
//		}
//		return false;
//	}
//
// }
