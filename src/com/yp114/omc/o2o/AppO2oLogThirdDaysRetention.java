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

public class AppO2oLogThirdDaysRetention extends Configured implements Tool {
	public static void main(String[] args) {
		/*
		 * 
		 * AppO2O 第3日留存率
		 */

		try {
			ToolRunner.run(new Configuration(), new AppO2oLogThirdDaysRetention(),
					args);
		} catch (Exception e) {
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {

			System.err.printf("Usage: %s <input><output>", getClass()
					.getSimpleName());

			ToolRunner.printGenericCommandUsage(System.err);

			return -1;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "AppO2oLogThirdDaysRetention");
		job.setJarByClass(AppO2oLogThirdDaysRetention.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AppO2oLogThirdDaysRetentionMapper.class);
		job.setCombinerClass(AppO2oLogThirdDaysRetentionReducer.class);
		job.setReducerClass(AppO2oLogThirdDaysRetentionReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class AppO2oLogThirdDaysRetentionMapper extends
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
		    //   第n日留存用户：  是当日新增uv里与 第n日uv     出现过2次的是留存用户
		    if( count == 2 ){
				// total uv  : o2o_id   
				String str1 = "#imei#";
				if (line.indexOf(str1) > 0) {
					ID = line.substring(0, line.indexOf(str1));
					// key: o2o_id channelno 留存率偏移量（天）
					context.write(new Text(ID + c + "A" + c + "3"  ),
							new IntWritable(1));
				}
				// total uv : o2o_id o2o_channel   
				String str2 = "#ii#";
				String str3 = "#chl#";
				if (line.indexOf(str2) > 0 && line.indexOf(str3) > 0) {
					ID = line.substring(0, line.indexOf(str3));
					CHL = line.substring(line.indexOf(str3) + 5, line.indexOf(str2));
					// key: o2o_id channelno 留存率偏移量（天）
					context.write(new Text(ID + c + CHL + c + "3" ),
							new IntWritable(1));	
			 }
			}

		}
	}

	public static class AppO2oLogThirdDaysRetentionReducer extends
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
