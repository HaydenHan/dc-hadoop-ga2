package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.DENullWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.MRConstants.PathConstant;
import com.dataeye.hadoop.domain.kv.MVAccountCreate;
import com.dataeye.hadoop.domain.kv.MVUIDActive;
import com.dataeye.hadoop.domain.result.DeviceActiveInfo;
import com.dataeye.hadoop.util.DCJsonUtil;
import com.dataeye.hadoop.util.StringUtil;

public class DeviceInfoProcessReducer extends Reducer<DEDynamicKV, DEDynamicKV, DEDynamicKV, DEDynamicKV> {

	private DEDynamicKV dynamicKey = new DEDynamicKV();
	private DEDynamicKV dynamicVal = new DEDynamicKV();
	private MultipleOutputs<DEDynamicKV, DEDynamicKV> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DEDynamicKV, DEDynamicKV>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	@Override
	protected void reduce(DEDynamicKV key, Iterable<DEDynamicKV> vals, Context context) throws IOException,
			InterruptedException {

		DeviceActiveInfo deviceActive = null;
		Set<String> accIdSet = new HashSet<String>();
		for (DEDynamicKV val : vals) {
			DEWritable instance = val.getDeWritable();
			if (instance instanceof DeviceActiveInfo) {
				deviceActive = (DeviceActiveInfo) instance;
			} else if (instance instanceof MVUIDActive) {
				MVUIDActive active = (MVUIDActive) instance;
				if (null == deviceActive) {
					deviceActive = new DeviceActiveInfo(active);
				} else if (MRConstants.INT_TRUE == active.getIsRealActive()) {
					deviceActive.setIsRealActive(MRConstants.INT_TRUE);
				}
			} else if (instance instanceof MVAccountCreate) {
				MVAccountCreate account = (MVAccountCreate) instance;
				accIdSet.add(account.getAccountId());
			}
		}

		if (null == deviceActive) {
			return;
		}

		if (accIdSet.size() > 0) {
			String accIds = DCJsonUtil.getGson().toJson(accIdSet);
			deviceActive.setIsActAndReg(MRConstants.INT_TRUE);
			deviceActive.setAccIdSet(accIds);
		}

		// 输出最终结果前设置时间为根据时区修正过的日期
		// deviceActive.setTimestamp(DCDateUtil.getStatDate(deviceActive));
		dynamicKey.setDeWritable(deviceActive);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_DEVICE_INFO));
	}

}
