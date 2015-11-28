package com.dataeye.hadoop.mapreduce.v2.role;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;
import com.dataeye.hadoop.util.DCDateUtil;
import com.dataeye.hadoop.util.HBaseUtil;
import com.dataeye.hadoop.util.RoleCacheUtil;

/**
 * <pre>
 * 逐天更新角色滚存信息
 * @author Hayden<br>
 * @date 2015年11月6日 下午4:25:44
 * <br>
 */
public class RoleInfoHBaseWriteMapper extends Mapper<LongWritable, Text, DEDynamicKV, DEDynamicKV> {

	private int dataDate = 0;

	// private DEDynamicKV dynamicKey = new DEDynamicKV();
	// private DEDynamicKV dynamicVal = new DEDynamicKV();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		dataDate = DCDateUtil.getDataDate(context.getConfiguration());
		HBaseUtil.init(context.getConfiguration());
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		RoleRollInfo info = new RoleRollInfo(value.toString());
		// dynamicKey.setDeWritable(info);
		// dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		try {
			RoleCacheUtil.updateRoleHistoryInfo(info, dataDate);
		} catch (Exception e) {
			System.out.println("write hbase failed:" + e.getMessage());
			System.out.println("recodes:" + value.toString());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
