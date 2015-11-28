package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.result.DeviceActiveInfo;
import com.dataeye.hadoop.util.DCDateUtil;
import com.dataeye.hadoop.util.DeviceCacheUtil;
import com.dataeye.hadoop.util.HBaseUtil;

public class DeviceInfoHBaseWriteMapper extends Mapper<LongWritable, Text, DEDynamicKV, DEDynamicKV> {

	private int dataDate = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		dataDate = DCDateUtil.getDataDate(context.getConfiguration());
		HBaseUtil.init(context.getConfiguration());
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		DeviceActiveInfo info = new DeviceActiveInfo(value.toString().split(MRConstants.FIELDS_SEPARATOR));
		if (info.getIsRealActive() > 0) {
			updateAccountHistoryInfo(info, context);
		}
	}

	private void updateAccountHistoryInfo(DeviceActiveInfo rollInfo, Context context) throws IOException,
			InterruptedException {
		DeviceCacheUtil.updateDeviceHistoryInfo(rollInfo, dataDate);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
