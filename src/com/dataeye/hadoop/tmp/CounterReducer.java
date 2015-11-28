package com.dataeye.hadoop.tmp;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CounterReducer extends Reducer<Person, Items, Person, Items> {

	private MultipleOutputs<Person, Items> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Person, Items>(context);
	}

	@Override
	protected void reduce(Person p, Iterable<Items> items, Context context) 
			throws IOException, InterruptedException {
		
		Items it = null;
		for(Items i : items){
			if(null == it){
				it = new Items(i.getCounts(), i.getMoney());
			}else{
				it.setCounts(it.getCounts() + i.getCounts());
				it.setMoney(it.getMoney() + i.getMoney());
			}
		}
		
		if("pony".equals(p.getName())){
			BigPerson bp = new BigPerson(p.getName(), p.getAge(), it.getCounts(), it.getMoney());
			mos.write("bigperson", bp, NullWritable.get(), "bigperson" + "/" + p.getName());
		}else{
			mos.write("person", p, it, p.getName() + "/" + p.getName());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

}
