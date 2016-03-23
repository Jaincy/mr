package com.yp114.omc.o2o;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

//import com.yp114.omc.utils.RegionUtil;

public class AppO2oLogProcessorTotal extends Configured implements Tool {

	public static void main(String[] args) {
		/*
		 *  
		 */

		try {
			ToolRunner.run(new Configuration(), new AppO2oLogProcessorTotal(),
					args);
		} catch (Exception e) {
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {

			System.err.printf("Usage: %s <input><input><output>", getClass()
					.getSimpleName());

			ToolRunner.printGenericCommandUsage(System.err);

			return -1;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "AppO2oLogProcessorTotal");
		job.setJarByClass(AppO2oLogProcessorTotal.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AppO2oLogMapper.class);
		job.setCombinerClass(AppO2oLogReducer.class);
		job.setReducerClass(AppO2oLogReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class AppO2oLogMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String string = value.toString();
			String[] split = string.split("\t");
			String line = split[0];
			int count = Integer.parseInt(split[1]);
			char c = 0x01;

			String ID = "";
			String CHL = "";
//			// pv : o2o_id
//			String str = "#id#";
//			if (line.indexOf(str) > 0) {
//				ID = line.substring(0, line.indexOf(str));
//				// key: o2o_id channelno is_imei is_total
//				context.write(new Text(ID + c + "A" + c + "0" + c + "0"),
//						new IntWritable(count));
//			}
//
//			// pv : o2o_id o2o_channel
//			String e = "#cc#";
//			if (line.indexOf(str) > 0 && line.indexOf(e) > 0) {
//				ID = line.substring(0, line.indexOf(str));
//				CHL = line.substring(line.indexOf(str) + 4, line.indexOf(e));
//				// key: o2o_id channelno is_imei is_total
//				context.write(new Text(ID + c + CHL + c + "0" + c + "0"),
//						new IntWritable(count));
//			}

			//total uv : o2o_id
			String str1 = "#imei#";
			if (line.indexOf(str1) > 0) {
				ID = line.substring(0, line.indexOf(str1));
				// key: o2o_id channelno is_imei is_total
				context.write(new Text(ID + c + "A" + c + "1" + c + "1"),
						new IntWritable(count));
			}
			// total uv : o2o_id o2o_channel
			String str2 = "#ii#";
			String str3 = "#chl#";
			if (line.indexOf(str2) > 0 && line.indexOf(str3) > 0) {
				ID = line.substring(0, line.indexOf(str3));
				CHL = line.substring(line.indexOf(str3) + 5, line.indexOf(str2));
				// key: o2o_id channelno is_imei is_total
				context.write(new Text(ID + c + CHL + c + "1" + c + "1"),
						new IntWritable(count));		 
			}
			
//			//total pv : o2o
//			String str4 = "#id#";
//			if (line.indexOf(str4) > 0) {
//				ID = line.substring(0, line.indexOf(str4));
//				// key: o2o_id channelno is_imei is_total
//				context.write(new Text(ID + c + "A" + c + "0" + c + "1"),
//						new IntWritable(count));
//			}
//			//total pv : o2o o2o_channel
//			String str5 = "#cc#";
//			if (line.indexOf(str5) > 0 && line.indexOf(str4) > 0) {
//				ID = line.substring(0, line.indexOf(str4));
//				CHL = line.substring(line.indexOf(str4) + 4, line.indexOf(str5));
//				// key: o2o_id channelno is_imei is_total
//				context.write(new Text(ID + c + CHL + c + "0" + c + "1"),
//						new IntWritable(count));
//			}

		

		}
	}

	public static class AppO2oLogReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			int sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}
