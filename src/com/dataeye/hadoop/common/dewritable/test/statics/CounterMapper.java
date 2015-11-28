package com.dataeye.hadoop.common.dewritable.test.statics;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<LongWritable, Text, Person, ItemBuyInfo> {

	Person p = new Person();
	ItemBuyInfo it = new ItemBuyInfo();
	
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
		context.write(p, it);
	}
}
