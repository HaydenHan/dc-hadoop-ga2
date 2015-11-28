package com.dataeye.hadoop.tmp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CounterMapper extends Mapper<LongWritable, Text, Person, Items> {

	private MultipleOutputs<Person, Items> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Person, Items>(context);
	}
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split("\t");
		int i = 0;
		String name = array[i++];
		int age = Integer.valueOf(array[i++]);
		int counts = Integer.valueOf(array[i++]);
		int money = Integer.valueOf(array[i++]);
		
		Person p = new Person(name, age);
		Items it = new Items(counts, money);
		
		if("waka".equals(p.getName())){
			mos.write("invalid", value, NullWritable.get(), "invalid" + "/" + p.getName());
		}else if("pony".equals(p.getName())){
			BigPerson bp = new BigPerson(p.getName(), p.getAge(), it.getCounts(), it.getMoney());
		}else{
			context.write(p, it);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
