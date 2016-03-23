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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AppO2oLogDayAddUser extends Configured implements Tool {
	public static void main(String[] args) {
		/*
		 * 
		 * AppO2O 当日新增用户
		 */

		try {
			ToolRunner.run(new Configuration(), new AppO2oLogDayAddUser(),
					args);
		} catch (Exception e) {
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {

			System.err.printf("Usage: %s <input><input><output>", getClass()
					.getSimpleName());

			ToolRunner.printGenericCommandUsage(System.err);

			return -1;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "AppO2oLogDayAddUser");
		job.setJarByClass(AppO2oLogDayAddUser.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(AppO2oLogDayAddUserMapper.class);
		job.setCombinerClass(AppO2oLogDayAddUserReducer.class);
		job.setReducerClass(AppO2oLogDayAddUserReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class AppO2oLogDayAddUserMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String string = value.toString();
			String[] split = string.split("\t");
			String line = split[0];
			int count = Integer.parseInt(split[1]);
 

			// total  uv  : o2o_id   今日 汇总uv 加   昨日汇总uv
			String str1 = "#imei#";
			if (line.indexOf(str1) > 0) {
				//ID = line.substring(0, line.indexOf(str1));
				context.write(new Text(line),
						new IntWritable(count));
			}
			//  total  uv  : o2o_id o2o_channel  今日 汇总uv 加   昨日汇总uv  
			String str2 = "#ii#";
			String str3 = "#chl#";
			if (line.indexOf(str2) > 0 && line.indexOf(str3) > 0) {
						context.write(new Text(line),
						new IntWritable(count));	
			}

		}
	}

	public static class AppO2oLogDayAddUserReducer extends
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
			//新增用户是当日汇总uv里与昨日汇总uv里只出现了一次的算新增用户，如果为2则为老用户
			if(sum == 1){
			context.write(key, new IntWritable(sum));
			}
//			if(sum == 2){
//				context.write(key, new IntWritable(sum));
//				}
		}
	}
}
