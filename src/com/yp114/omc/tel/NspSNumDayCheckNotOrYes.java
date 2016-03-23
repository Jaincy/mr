package com.yp114.omc.tel;

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

import com.yp114.omc.utils.RegionUtil;

public class NspSNumDayCheckNotOrYes extends Configured implements Tool {

	public static void main(String args[]){
		try {
			ToolRunner.run(new NspSNumDayCheckNotOrYes(), args);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		  conf.set("dfs.client.block.write.replace-datanode-on-failure.policy",
	                "NEVER"
	        ); 

	conf.set("dfs.client.block.write.replace-datanode-on-failure.enable",
	                "true"
	        ); 
		Job job = Job.getInstance(conf, "NspSNumDayCheckNotOrYes");
		job.setJarByClass(NspSNumDayCheckNotOrYes.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AnalysisMapper.class);
		job.setCombinerClass(ConReducer.class);
		job.setReducerClass(ConReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class AnalysisMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			IntWritable one = new IntWritable(1);
			Text word = new Text();
			String line = value.toString();
			// 获得日志中的地区代码
			String areacode = "noAreacode";
			int areacodeIndex = line.indexOf("areacode\":\"");
			int areacodeEndIndex = line.indexOf("\",\"countAll\"");

			if (areacodeIndex > 0 && areacodeEndIndex > 0) {
				areacode = line.substring(areacodeIndex + 11, areacodeEndIndex);
			}

			// System.out.println(areacode);

			if (areacode.length() > 2) {
				areacode = areacode.substring(0, 2);
			}

			String regionCode = RegionUtil.getRegionCode(areacode);
			// 获得日志中的请求类型
			String requestType = "X";
			int requesttypeStartIndex = line.indexOf("requesttype\":\"");
			int requesttypeEndIndex = line.indexOf("\",\"responsecode");

			if (requesttypeStartIndex > 0 && requesttypeEndIndex > 0) {
				requestType = line.substring(requesttypeStartIndex + 14,
						requesttypeEndIndex);
			}
			// 获得日志中的responsecode
			String responsecode = "X";
			int responsecodeStartIndex = line.indexOf("responsecode\":\"");
			int responsecodeEndIndex = line.indexOf("\",\"responsedata");
			if (responsecodeStartIndex > 0 && responsecodeEndIndex > 0) {
				responsecode = line.substring(responsecodeStartIndex + 15,
						responsecodeEndIndex);
			}
			// 获得日志中的subjectNum
			String subjectNum = "X";

			int subjectNumStartIndex = line.indexOf("subjectNum=");
			int subjectNumEndIndex = line.indexOf(", userAgent");

			if (subjectNumStartIndex > 0 && subjectNumEndIndex > 0) {
				subjectNum = line.substring(subjectNumStartIndex + 11,
						subjectNumEndIndex);
			}
			// 1、表示有           0、表示无
			String yesOrNo = "1";
			// 判断记录是有还是无
			if (responsecode.equals("010005")) {
				yesOrNo = "0";
			}
			
			word.set(regionCode+"#r#"+requestType+"#s#"+subjectNum+"#c#"+responsecode+"#y#"+yesOrNo);
			context.write(word, one);
		}
	}

	public static class ConReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
