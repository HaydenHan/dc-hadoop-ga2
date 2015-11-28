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
import com.dataeye.hadoop.domain.result.role.RoleBackInfo;
import com.dataeye.hadoop.domain.result.role.RoleDayInfo;
import com.dataeye.hadoop.domain.result.role.RoleLTVInfo;
import com.dataeye.hadoop.domain.result.role.RoleRetainInfo;
import com.dataeye.hadoop.mapreduce.v2.role.RoleInfoHBaseWriteMapper;

public class RoleInfoHbaseWriteMapTest {
	private MapDriver<LongWritable, Text, DEDynamicKV, DEDynamicKV> mapDriver;
	private String logFileLocalPath = "D:/logs/roleRolling-r-00003";

	private String roleDayInfoResult = "D:/logs/result-roleDayInfo.log";
	private String roleRetainInfoResult = "D:/logs/result-roleRetainInfo.log";
	private String roleBackInfoResult = "D:/logs/result-roleBackInfo.log";
	private String roleLTVInfoResult = "D:/logs/result-roleLTVInfo.log";

	@Before
	public void setUp() {
		RoleInfoHBaseWriteMapper map = new RoleInfoHBaseWriteMapper();
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
		// mapDriver.runTest();

		DataOutputStream roleRetainInfoOut = new DataOutputStream(new FileOutputStream(new File(roleRetainInfoResult)));

		DataOutputStream roleBackInfoOut = new DataOutputStream(new FileOutputStream(new File(roleBackInfoResult)));

		DataOutputStream roleLTVInfoOut = new DataOutputStream(new FileOutputStream(roleLTVInfoResult));
		DataOutputStream roleDayInfoOut = new DataOutputStream(new FileOutputStream(roleDayInfoResult));

		for (Pair<DEDynamicKV, DEDynamicKV> pair : onlineList) {
			if (pair.getFirst().getDeWritable() instanceof RoleDayInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleDayInfoOut);
				roleDayInfoOut.write("\n".getBytes("UTF-8"));
			} else if (pair.getFirst().getDeWritable() instanceof RoleRetainInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleRetainInfoOut);
				roleRetainInfoOut.write("\n".getBytes("UTF-8"));
			} else if (pair.getFirst().getDeWritable() instanceof RoleBackInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleBackInfoOut);
				roleBackInfoOut.write("\n".getBytes("UTF-8"));
			} else if (pair.getFirst().getDeWritable() instanceof RoleLTVInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleLTVInfoOut);
				roleLTVInfoOut.write("\n".getBytes("UTF-8"));
			}
		}
		System.out.println("Map,End------------------------------------------------------------------");
	}
}
