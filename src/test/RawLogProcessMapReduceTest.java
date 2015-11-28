package test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.kv.AccountRollInfo;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;
import com.dataeye.hadoop.mapreduce.v2.RawLogProcessMapper;
import com.dataeye.hadoop.mapreduce.v2.RawLogProcessReducer;

public class RawLogProcessMapReduceTest {
	private MapReduceDriver<LongWritable, Text, DEDynamicKV, DEDynamicKV, DEDynamicKV, DEDynamicKV> mapReduceDriver;
	private MapDriver<LongWritable, Text, DEDynamicKV, DEDynamicKV> mapDriver;
	private String logFileHdfsPath = "hdfs://m2:9000/data/digitcube-h5/acupcu/2015/08/18/00/output/part-r-00000-ACU_PCU_CNT";
	private String logFileLocalPath = "D:/logs/logdata-2015-11-16-19-1.log_vm2";
	private String mapResultFile = "D:/logs/logdata-2015-10-19-21-map-result.log";
	private String accRollResult = "D:/logs/logdata-accountRolling.log";
	private String roleRollResult = "D:/logs/logdata-roleRolling.log";

	@Before
	public void setUp() {
		RawLogProcessMapper map = new RawLogProcessMapper();
		RawLogProcessReducer red = new RawLogProcessReducer();
		mapDriver = MapDriver.newMapDriver(map);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, red);
	}

	@Test
	public void testMap() throws Exception {
		System.out.println("Map,Begin------------------------------------------------------------------");
		LongWritable longWritable = new LongWritable();
		for (String data : TestDataUtil.readFromLocalLog(logFileLocalPath)) {
			mapDriver.withInput(longWritable, new Text(data));
		}
		List<Pair<DEDynamicKV, DEDynamicKV>> resultList = mapDriver.run();
		File outFile = new File(mapResultFile);
		DataOutputStream out = new DataOutputStream(new FileOutputStream(outFile));
		for (Pair<DEDynamicKV, DEDynamicKV> pair : resultList) {
			out.write("MapKey:\n".getBytes("UTF-8"));
			pair.getFirst().getDeWritable().writeAsString(out);
			out.write("\nMapValue:\n".getBytes("UTF-8"));
			pair.getSecond().getDeWritable().writeAsString(out);
			out.write("\n".getBytes("UTF-8"));
		}
		out.close();
		System.out.println("Map,End------------------------------------------------------------------");
	}

	@Test
	public void testReduce() throws Exception {
		System.out.println("Reduce,Begin------------------------------------------------------------------");
		LongWritable longWritable = new LongWritable();
		for (String data : TestDataUtil.readFromLocalLog(logFileLocalPath)) {
			mapReduceDriver.withInput(longWritable, new Text(data));
		}
		List<Pair<DEDynamicKV, DEDynamicKV>> resultList = mapReduceDriver.run();
		DataOutputStream accountRollOut = new DataOutputStream(new FileOutputStream(new File(accRollResult)));
		DataOutputStream roleRollOut = new DataOutputStream(new FileOutputStream(new File(roleRollResult)));
		for (Pair<DEDynamicKV, DEDynamicKV> pair : resultList) {
			if (pair.getFirst().getDeWritable() instanceof AccountRollInfo) {
				pair.getFirst().getDeWritable().writeAsString(accountRollOut);
				accountRollOut.write("\n".getBytes("UTF-8"));
			} else if (pair.getFirst().getDeWritable() instanceof RoleRollInfo) {
				pair.getFirst().getDeWritable().writeAsString(roleRollOut);
				roleRollOut.write("\n".getBytes("UTF-8"));
			}
		}
		System.out.println("Reduce,End------------------------------------------------------------------");
	}
}
