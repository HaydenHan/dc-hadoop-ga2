package com.dataeye.hadoop.tmp.v2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<LongWritable, Text, DCDynamicKV, DCDynamicKV> {

	Person p = new Person();
	Items it = new Items();
	DCDynamicKV keyObj = new DCDynamicKV(p);
	DCDynamicKV val = new DCDynamicKV(it);
	
	ItemName itn = new ItemName();
	ItemUser itu = new ItemUser();
	DCDynamicKV keyObj2 = new DCDynamicKV(itn);
	DCDynamicKV val2 = new DCDynamicKV(itu);
	
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
