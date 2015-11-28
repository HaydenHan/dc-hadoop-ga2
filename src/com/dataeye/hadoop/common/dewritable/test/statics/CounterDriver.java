package com.dataeye.hadoop.common.dewritable.test.statics;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DCOutputFormat;

public class CounterDriver {

	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		String outputPath = args[1];
		
		Job job = new Job();
		job.setJobName("Person");
		job.setJarByClass(CounterDriver.class);
		job.setMapperClass(CounterMapper.class);
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(ItemBuyInfo.class);
		job.setReducerClass(CounterReducer.class);
		job.setOutputKeyClass(Person.class);
		job.setOutputValueClass(ItemBuyInfo.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		MultipleOutputs.addNamedOutput(job, "person", DCOutputFormat.class, Person.class, ItemBuyInfo.class);
		
		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}
