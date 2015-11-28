package com.dataeye.hadoop.common.dewritable.test.dynamic;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;

public class CounterMapper extends Mapper<LongWritable, Text, DEDynamicKV, DEDynamicKV> {

	Person p = new Person();
	ItemBuyInfo it = new ItemBuyInfo();
	DEDynamicKV keyObj = new DEDynamicKV(p);
	DEDynamicKV val = new DEDynamicKV(it);
	
	ItemName itn = new ItemName();
	ItemUserInfo itu = new ItemUserInfo();
	DEDynamicKV keyObj2 = new DEDynamicKV(itn);
	DEDynamicKV val2 = new DEDynamicKV(itu);
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split("\t");
		int i = 0;
		String name = array[i++];
		int age = Integer.valueOf(array[i++]);
		int counts = Integer.valueOf(array[i++]);
		int money = Integer.valueOf(array[i++]);
		
		p.setName(name);
		p.setAge(age);
		
		it.setCounts(counts);
		it.setMoney(money);
		context.write(keyObj, val);
		
		itn.setName(""+money);
		itu.setUsername(name);
		itu.setCount(counts);
		itu.setMoney(money);
		context.write(keyObj2, val2);
	}
}
