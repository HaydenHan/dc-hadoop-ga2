package test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.result.AccountBackInfo;
import com.dataeye.hadoop.domain.result.AccountDayInfo;
import com.dataeye.hadoop.domain.result.AccountLTVInfo;
import com.dataeye.hadoop.domain.result.AccountRetainInfo;
import com.dataeye.hadoop.mapreduce.v2.AccountInfoProcessMapper;

public class AccountInfoProcessMapReduceTest {
	private MapDriver<LongWritable, Text, DEDynamicKV, DEDynamicKV> mapDriver;
	private String logFileLocalPath = "D:/logs/accountRolling-r-00002";

	private String accountDayInfoResult = "D:/logs/result-raccountDayInfo.log";
	private String accountRetainInfoResult = "D:/logs/result-accountRetainInfo.log";
	private String accountBackInfoResult = "D:/logs/result-accountBackInfo.log";
	private String accountLTVInfoResult = "D:/logs/result-accountLTVInfo.log";

	@Before
	public void setUp() {
		AccountInfoProcessMapper map = new AccountInfoProcessMapper();
		mapDriver = MapDriver.newMapDriver(map);
	}

	@Test
	public void testMap() throws Exception {
		System.out.println("Map,Begin------------------------------------------------------------------");
		LongWritable longWritable = new LongWritable();
		for (String data : TestDataUtil.readFromLocalLog(logFileLocalPath)) {
			mapDriver.withInput(longWritable, new Text(data));
		}
		List<Pair<DEDynamicKV, DEDynamicKV>> onlineList = mapDriver.run();

		DataOutputStream roleDayInfoOut = new DataOutputStream(new FileOutputStream(new File(accountDayInfoResult)));

		DataOutputStream roleRetainInfoOut = new DataOutputStream(new FileOutputStream(
				new File(accountRetainInfoResult)));

		DataOutputStream roleBackInfoOut = new DataOutputStream(new FileOutputStream(new File(accountBackInfoResult)));

		DataOutputStream roleLTVInfoOut = new DataOutputStream(new FileOutputStream(accountLTVInfoResult));

		for (Pair<DEDynamicKV, DEDynamicKV> pair : onlineList) {
			if (pair.getFirst().getDeWritable() instanceof AccountDayInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleDayInfoOut);
				roleDayInfoOut.write("\n".getBytes("UTF-8"));
			} else if (pair.getFirst().getDeWritable() instanceof AccountRetainInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleRetainInfoOut);
				roleRetainInfoOut.write("\n".getBytes("UTF-8"));
			} else if (pair.getFirst().getDeWritable() instanceof AccountBackInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleBackInfoOut);
				roleBackInfoOut.write("\n".getBytes("UTF-8"));
			} else if (pair.getFirst().getDeWritable() instanceof AccountLTVInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleLTVInfoOut);
				roleLTVInfoOut.write("\n".getBytes("UTF-8"));
			}
		}
		System.out.println("Map,End------------------------------------------------------------------");
	}
}
