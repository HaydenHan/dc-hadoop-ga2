package com.dataeye.hadoop.common.dewritable.test.statics;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CounterReducer extends Reducer<Person, ItemBuyInfo, Person, ItemBuyInfo> {

	private MultipleOutputs<Person, ItemBuyInfo> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Person, ItemBuyInfo>(context);
	}

	@Override
	protected void reduce(Person key, Iterable<ItemBuyInfo> vals, Context context) 
			throws IOException, InterruptedException {
		
		int totalCount = 0;
		int totalMoney = 0;
		for(ItemBuyInfo it : vals){
			totalCount += it.getCounts();
			totalMoney += it.getMoney();
		}
		ItemBuyInfo it = new ItemBuyInfo(totalCount, totalMoney);
		mos.write("person", key, it, "person/person");
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

}
