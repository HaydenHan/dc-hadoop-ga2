package com.dataeye.hadoop.tmp.v2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import com.dataeye.hadoop.tmp.DCOutputFormat;

public class CounterDriver {

	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		String outputPath = args[1];
		
		Job job = new Job();
		job.setJobName("Person");
		job.setJarByClass(CounterDriver.class);
		job.setMapperClass(CounterMapper.class);
		job.setMapOutputKeyClass(DCDynamicKV.class);
		job.setMapOutputValueClass(DCDynamicKV.class);
		job.setReducerClass(CounterReducer.class);
		job.setOutputKeyClass(DCDynamicKV.class);
		job.setOutputValueClass(DCDynamicKV.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		MultipleOutputs.addNamedOutput(job, "person", DCOutputFormat.class, DCDynamicKV.class, DCDynamicKV.class);
		MultipleOutputs.addNamedOutput(job, "item", DCOutputFormat.class, DCDynamicKV.class, DCDynamicKV.class);
		
		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}
