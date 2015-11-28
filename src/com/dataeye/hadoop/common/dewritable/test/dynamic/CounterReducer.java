package com.dataeye.hadoop.common.dewritable.test.dynamic;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;

public class CounterReducer extends Reducer<DEDynamicKV, DEDynamicKV, DEDynamicKV, DEDynamicKV> {

	private MultipleOutputs<DEDynamicKV, DEDynamicKV> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DEDynamicKV, DEDynamicKV>(context);
	}

	@Override
	protected void reduce(DEDynamicKV key, Iterable<DEDynamicKV> vals, Context context) 
			throws IOException, InterruptedException {
		if(key.getDeWritable() instanceof Person){
			int totalCount = 0;
			int totalMoney = 0;
			for(DEDynamicKV val : vals){
				ItemBuyInfo it = (ItemBuyInfo)val.getDeWritable();
				totalCount += it.getCounts();
				totalMoney += it.getMoney();
			}
			ItemBuyInfo it = new ItemBuyInfo(totalCount, totalMoney);
			mos.write("person", key, it, "person/person");
			
		}else if(key.getDeWritable() instanceof ItemName){
			Set<String> accSet = new HashSet<String>();
			int totalCount = 0;
			int totalMoney = 0;
			for(DEDynamicKV val : vals){
				ItemUserInfo it = (ItemUserInfo)val.getDeWritable();
				accSet.add(it.getUsername());
				totalCount += it.getCount();
				totalMoney += it.getMoney();
			}
			
			ItemSumInfo is = new ItemSumInfo();
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
