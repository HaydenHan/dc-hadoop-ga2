package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.util.DCDateUtil;

public abstract class DEBaseMapper extends Mapper<LongWritable, Text, DEDynamicKV, DEDynamicKV> {

	private DEDynamicKV dynamicKey = new DEDynamicKV();
	private DEDynamicKV dynamicVal = new DEDynamicKV();
	private MultipleOutputs<DEDynamicKV, DEDynamicKV> mos;

	private int dataDate = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		dataDate = DCDateUtil.getDataDate(context.getConfiguration());
		mos = new MultipleOutputs<DEDynamicKV, DEDynamicKV>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	public abstract void deMap(LongWritable key, Text value, Context context) throws IOException, InterruptedException;

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		deMap(key, value, context);
	}

}
