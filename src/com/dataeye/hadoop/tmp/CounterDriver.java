package com.dataeye.hadoop.tmp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CounterDriver {

	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		String outputPath = args[1];
		
		/*Configuration conf = new Configuration();
		conf.set("mapreduce.map.class", "com.dataeye.hadoop.tmp.CounterMapper");
		conf.set("mapred.mapoutput.key.class", "com.dataeye.hadoop.tmp.Person");
		conf.set("mapred.mapoutput.value.class", "com.dataeye.hadoop.tmp.Items");
		conf.set("mapreduce.reduce.class", "com.dataeye.hadoop.tmp.CounterReducer");
		conf.set("mapred.output.key.class", "com.dataeye.hadoop.tmp.Person");
		conf.set("mapred.output.value.class", "com.dataeye.hadoop.tmp.Items");
		conf.set("mapred.reduce.tasks", "1");
		conf.set("mapred.input.dir", inputPath);
		conf.set("mapred.output.dir", outputPath);
		Job job = new Job(conf, "Person");*/
		
		Job job = new Job();
		job.setJobName("Person");
		job.setJarByClass(CounterDriver.class);
		job.setMapperClass(CounterMapper.class);
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(Items.class);
		job.setReducerClass(CounterReducer.class);
		job.setOutputKeyClass(Person.class);
		job.setOutputValueClass(Items.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		MultipleOutputs.addNamedOutput(job, "person", DCOutputFormat.class, Person.class, Items.class);
		MultipleOutputs.addNamedOutput(job, "bigperson", DCOutputFormat.class, BigPerson.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "invalid", DCOutputFormat.class, Text.class, NullWritable.class);
		
		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}
