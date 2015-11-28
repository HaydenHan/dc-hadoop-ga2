package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;

public abstract class DEBaseReducer extends Reducer<DEDynamicKV, DEDynamicKV, DEDynamicKV, DEDynamicKV> {

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

	public abstract void deReduce(DEDynamicKV key, Iterable<DEDynamicKV> vals, Context context) throws IOException,
			InterruptedException;

	@Override
	protected void reduce(DEDynamicKV key, Iterable<DEDynamicKV> vals, Context context) throws IOException,
			InterruptedException {
		deReduce(key, vals, context);
	}

}
