package com.dataeye.hadoop.tmp.v2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CounterReducer extends Reducer<DCDynamicKV, DCDynamicKV, DCDynamicKV, DCDynamicKV> {

	private MultipleOutputs<DCDynamicKV, DCDynamicKV> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DCDynamicKV, DCDynamicKV>(context);
	}

	@Override
	protected void reduce(DCDynamicKV key, Iterable<DCDynamicKV> vals, Context context) 
			throws IOException, InterruptedException {
		if(key.getDcWritable() instanceof Person){
			int totalCount = 0;
			int totalMoney = 0;
			for(DCDynamicKV val : vals){
				Items it = (Items)val.getDcWritable();
				totalCount += it.getCounts();
				totalMoney += it.getMoney();
			}
			Items it = new Items(totalCount, totalMoney);
			mos.write("person", key, it, "person/person");
			
		}else if(key.getDcWritable() instanceof ItemName){
			Set<String> accSet = new HashSet<String>();
			int totalCount = 0;
			int totalMoney = 0;
			for(DCDynamicKV val : vals){
				ItemUser it = (ItemUser)val.getDcWritable();
				accSet.add(it.getUsername());
				totalCount += it.getCount();
				totalMoney += it.getMoney();
			}
			
			ItemSum is = new ItemSum();
			is.setUsercount(accSet.size());
			is.setCount(totalCount);
			is.setMoney(totalMoney);
			
			mos.write("item", key, is, "item/item");
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

}
