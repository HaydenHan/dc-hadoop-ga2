//package com.dataeye.hadoop.util;
//
//import java.io.Closeable;
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//
//public class HbaseConnectionManager {
//
////	private static String hbase_zookeeper_quorum = "dchdmaster1,dchdmaster2,dcjournalnode1";
//	private static String hbase_zookeeper_quorum = "hadoop3,server1,server2";
//	private static String hbase_zookeeper_property_clientPort = "12181";
//	private static Configuration configuration;
//
//	private static Connection connection = null;
//
//	public static Connection getConnection() throws IOException {
//		configuration = HBaseConfiguration.create();
//		configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
//		configuration.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_property_clientPort);
//		connection = ConnectionFactory.createConnection(configuration);
//		return connection;
//	}
//
//	public static void closeAllConns(){
//		try {
//			if (connection != null)
//				connection.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//	
//	public static void close(Closeable table) {
//		try {
//			if (table != null)
//				table.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	public static void close(Connection connection) {
//		try {
//			if (connection != null)
//				connection.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//	
//	public static void close(Closeable table, Connection connection) {
//		try {
//			if (table != null)
//				table.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		try {
//			if (connection != null)
//				connection.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//	
// }
