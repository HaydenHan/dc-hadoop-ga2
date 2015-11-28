package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.MRConstants.PathConstant;
import com.dataeye.hadoop.domain.kv.MKUID;
import com.dataeye.hadoop.domain.kv.MVAccountCreate;
import com.dataeye.hadoop.domain.kv.MVUIDActive;
import com.dataeye.hadoop.domain.result.DeviceActiveInfo;
import com.dataeye.hadoop.util.DCDateUtil;

public class DeviceInfoProcessMapper extends Mapper<LongWritable, Text, DEDynamicKV, DEDynamicKV> {

	private DEDynamicKV dynamicKey = new DEDynamicKV();
	private DEDynamicKV dynamicVal = new DEDynamicKV();

	private boolean isDeviceActive = false;
	private boolean isAccountCreate = false;
	private boolean isDeviceInfo = false;
	private boolean isNeedLastHourRolling = true;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		isNeedLastHourRolling = DCDateUtil.isNeedLastHourRolling(context.getConfiguration());
		String fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		if (fileSuffix.startsWith(PathConstant.BUSINESS_DEVICE_ACTIVE)) {
			isDeviceActive = true;
		} else if (fileSuffix.startsWith(PathConstant.BUSINESS_ACCOUNT_CREATE)) {
			isAccountCreate = true;
		} else if (fileSuffix.startsWith(PathConstant.BUSINESS_DEVICE_INFO)) {
			isDeviceInfo = true;
		}
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(MRConstants.FIELDS_SEPARATOR);

		if (isDeviceActive) {
			MVUIDActive uidActive = new MVUIDActive(fields);
			MKUID uid = new MKUID(uidActive.getAppId(), uidActive.getPlatform(), uidActive.getUid());
			dynamicKey.setDeWritable(uid);
			dynamicVal.setDeWritable(uidActive);
			context.write(dynamicKey, dynamicVal);
		} else if (isAccountCreate) {
			MVAccountCreate accCreate = new MVAccountCreate(fields);
			MKUID uid = new MKUID(accCreate.getAppId(), accCreate.getPlatform(), accCreate.getUid());
			dynamicKey.setDeWritable(uid);
			dynamicVal.setDeWritable(accCreate);
			context.write(dynamicKey, dynamicVal);
		} else if (isDeviceInfo) {
			if (!isNeedLastHourRolling) {
				return;
			}
			DeviceActiveInfo deviceActiveInfo = new DeviceActiveInfo(fields);
			MKUID uid = new MKUID(deviceActiveInfo.getAppId(), deviceActiveInfo.getPlatform(),
					deviceActiveInfo.getUid());
			dynamicKey.setDeWritable(uid);
			dynamicVal.setDeWritable(deviceActiveInfo);
			context.write(dynamicKey, dynamicVal);
		}
	}
}
