package com.yp114.omc.o2o;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

//import com.yp114.omc.utils.RegionUtil;

public class AppO2oLogProcessorHour extends Configured implements Tool {

	public static void main(String[] args) {
		/*
		 * 
		 * AppO2o日志 第一步 日志清洗
		 */

		try {
			ToolRunner.run(new Configuration(), new AppO2oLogProcessorHour(),
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
		// 启用输出文件压缩
		conf.setBoolean("mapred.output.compress", true);
		// // 选择压缩器
		// conf.setClass("mapred.output.compression.codec", GzipCodec.class,
		// CompressionCodec.class);
		conf.setClass("mapred.output.compression.codec", BZip2Codec.class,
				CompressionCodec.class);
		// conf.setClass("mapred.output.compression.codec", SnappyCodec.class,
		// CompressionCodec.class);

		Job job = Job.getInstance(conf, "AppO2oLogProcessorHour");

		job.setJarByClass(AppO2oLogProcessorHour.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AppO2oLogHourMapper.class);
		job.setCombinerClass(AppO2oLogHourReducer.class);
		job.setReducerClass(AppO2oLogHourReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class AppO2oLogHourMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			line = StringUtils.replaceBlank(line);
			if (line.length() > 0) {

				// logId
				String logid = "nologid";
				String day_id = "";
				String hour_id = "";
				int logidStartIndex = line.indexOf("logid\":\"");
				// int logidEndIndex = line.indexOf("\",\"requestinfo");
				int logidEndIndex = line.substring(logidStartIndex + 8)
						.indexOf("\"");
				if (logidStartIndex > 0 && logidEndIndex > 0) {
					logid = line.substring(logidStartIndex + 8, logidStartIndex
							+ 8 + logidEndIndex);
					day_id = line.substring(logidStartIndex + 8,
							logidStartIndex + 8 + 8);
					hour_id = line.substring(logidStartIndex + 8 + 8,
							logidStartIndex + 8 + 8 + 2);
				}
				//System.out.println(logid);

				// areacode
				String areacode = "noAreacode";
				int areacodeStartIndex = line.indexOf("areacode\":\"");
				// int areacodeEndIndex = line.indexOf("\",\"logid\"");
				int areacodeEndIndex = line.substring(areacodeStartIndex + 11)
						.indexOf("\"");
				if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
					areacode = line.substring(areacodeStartIndex + 11,
							areacodeStartIndex + 11 + areacodeEndIndex);
				}
				//System.out.println(areacode);
				// if (areacode.length() > 2) {
				// areacode = areacode.substring(0, 2);
				// }
				// String regionCode = RegionUtil.getRegionCode(areacode);
				// System.out.println(regionCode);

				// ID
				String ID = "null";
				int idStartIndex = line.indexOf("\\\"ID\\\":\\\"");
				// int idEndIndex = line.indexOf("\\\",\\\"NAME");
				int idEndIndex = line.substring(idStartIndex + 9).indexOf(
						"\\\"");
				if (idStartIndex > 0 && idEndIndex > 0) {
					ID = line.substring(idStartIndex + 9, idStartIndex + 9
							+ idEndIndex);
				} else {
					ID = "null";
				}
				//System.out.println(ID);

				// name
				String name = "noName";
				int nameStartIndex = line.indexOf("\\\"NAME\\\":\\\"");
				// int nameEndIndex = line.indexOf("\\\",\\\"NOTE");
				int nameEndIndex = line.substring(nameStartIndex + 11).indexOf(
						"\\\"");
				if (nameStartIndex > 0 && nameEndIndex > 0) {
					name = line.substring(nameStartIndex + 11, nameStartIndex
							+ 11 + nameEndIndex);
				}
				//System.out.println(name);

				// note
				// String note = "noNote";
				// int noteStartIndex = line.indexOf("NOTE\\\":\\\"");
				// int noteEndIndex = line.indexOf("\\\",\\\"channelno");
				// if (noteStartIndex > 0 && noteEndIndex > 0) {
				// note = line.substring(noteStartIndex + 9, noteEndIndex);
				// }
				// System.out.println(note);

				// channelno
				String channelno = "noChannel";
				int channelnoStartIndex = line.indexOf("channelno\\\":\\\"");
				// int channelnoEndIndex = line.indexOf("\\\",\\\"imei");
				int channelnoEndIndex = line
						.substring(channelnoStartIndex + 14).indexOf("\\\"");
				if (channelnoStartIndex > 0 && channelnoEndIndex > 0) {
					channelno = line.substring(channelnoStartIndex + 14,
							channelnoStartIndex + 14 + channelnoEndIndex);
				}
				//System.out.println(channelno);

				// imei
				String imei = "noImei";
				int imeiStartIndex = line.indexOf("imei\\\":\\\"");
				// int imeiEndIndex = line.indexOf("\\\",\\\"queryNum");
				int imeiEndIndex = line.substring(imeiStartIndex + 9).indexOf(
						"\\\"");
				if (imeiStartIndex > 0 && imeiEndIndex > 0) {
					imei = line.substring(imeiStartIndex + 9, imeiStartIndex
							+ 9 + imeiEndIndex);
				}
				//System.out.println(imei);

				// 组装
				String c = "\t"; // 分隔符
				String str = day_id + c + hour_id + c + ID + c + name + c
						+ channelno + c + imei + c + areacode;
				word.set(str);
				// if(logid.length() > 0 && day_id.length() > 0 &&
				// hour_id.length() > 0){
				context.write(word, one);
				// }

			}

		}
	}

	public static class AppO2oLogHourReducer extends
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
