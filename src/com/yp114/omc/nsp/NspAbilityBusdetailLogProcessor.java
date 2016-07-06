package com.yp114.omc.nsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.yp114.omc.utils.LatnUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import com.yp114.omc.ip.IpSearch;

import com.yp114.omc.utils.RegionUtil;
import org.json.JSONException;
import org.json.JSONObject;

public class NspAbilityBusdetailLogProcessor extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "NspAbilityBusdetailLogProcessor");
		job.setJarByClass(NspAbilityBusdetailLogProcessor.class);

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		job.setMapperClass(NspAbilityBusdetailLogMapper.class);
		job.setCombinerClass(NspAbilityBusdetailLogReducer.class);
		job.setReducerClass(NspAbilityBusdetailLogReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {

		try {
			ToolRunner.run(new NspAbilityBusdetailLogProcessor(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static class NspAbilityBusdetailLogMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			
			if (line.contains("{")&&line.substring(line.indexOf("{")).length() > 0) {
				try {
					String date_time = line.substring(0, 13);

					String[] split = date_time.split(" ");
					String hour_id = "NoHour";
					String day_id = "Noday";
					if (split.length >= 2) {
						hour_id = split[split.length - 1];
						day_id = split[split.length - 2];
						day_id = day_id.replaceAll("-", "");
					}
					

						JSONObject jso = new JSONObject(line.substring(line.indexOf("{")));
						// 功能点的Value
						String subjectNum = "noValue";
						// 区域
						String region = "noRegion";
						// 商家ID
						String shopid = "noShopid";
						// 商家名
						String shopname = "noShopname";
						// 渠道号
						String channelno = "noChannelno";
						// 商家名
						String name = "noName";
						if ((line.contains("requestinfo"))
								&& (jso.getString("requestinfo")
										.startsWith("{"))) {
							JSONObject requestjso = new JSONObject(
									jso.getString("requestinfo"));

							if (line.contains("value")) {
								subjectNum = requestjso.getString("value");
							}
							if (line.contains("region")) {
								region = requestjso.getString("region");
							}
							if (line.contains("shopid")) {
								shopid = requestjso.getString("shopid");
							}

							if (line.contains("shopname")) {
								shopname = requestjso.getString("shopname");
							}

							if (line.contains("channelno")) {
								channelno = requestjso.getString("channelno");
								if (channelno.length() >= 4) {
									channelno = "noChannelno";
								}
							}
							if (line.contains("name")) {
								name = requestjso.getString("name");
							}

						}
						String responsecode = "noErrorCode";
						if ((line.contains("responseinfo"))
								&& (jso.getString("responseinfo")
										.startsWith("{"))) {
							JSONObject responseinfo = new JSONObject(
									jso.getString("responseinfo"));
							// 请求编码
							if (line.contains("errorCode")) {
								responsecode = responseinfo
										.getString("errorCode");
							}
						}
						// 请求IP
						String requestip = "noRequestIp";
						if (line.contains("requestip")) {
							requestip = jso.getString("requestip");
						}
						// 省
						String province = "noProvince";
						// 城市
						String city = "noCity";
						// 县
						String conty = "noConty";
						// 市编码
						String latnCode = "nolatnCode";
						// 省编码
						String provinceCode = "noProvinceCode";

						List<String> zcityList = new ArrayList<String>();
						zcityList.add("澳门");
						zcityList.add("台湾");
						zcityList.add("香港");
						zcityList.add("海南");
						zcityList.add("天津");
						zcityList.add("北京");
						zcityList.add("重庆");
						List<String> zcontyList = new ArrayList<String>();
						zcontyList.add("济源");
						zcontyList.add("仙桃");
						zcontyList.add("潜江");
						zcontyList.add("天门");
						zcontyList.add("神农架");
						zcontyList.add("阿拉尔");
						zcontyList.add("石河子");
						
						String regex = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
					if (requestip.matches(regex)) {
						IpSearch finder = IpSearch.getInstance();
						String result = finder.Get(requestip);
						result = result.toString().trim().replaceAll("\t", "");
						String[] splits = result.split("\\|");

						if (splits.length == 11) {
							province = splits[2];
							city = splits[3];
							conty = splits[4];
							if (zcityList.contains(province)) {
								city = province;
							} else if (zcontyList.contains(conty)) {
								city = conty;
							} else if ("".equals(city)) {
								city = province;
							}

							if (province.length() > 2) {
								province = province.substring(0, 2);
							}

							latnCode = LatnUtil.getRegionCode(city);
							provinceCode = RegionUtil.getRegionCode(province);
						}
						System.out.println(requestip);

					}
						String reducekey = day_id + "\t" + hour_id + "\t"
								+ subjectNum + "\t" + region + "\t" + shopid
								+ "\t" + shopname + "\t" + channelno + "\t"
								+ name + "\t" + requestip + "\t" + latnCode
								+ "\t" + provinceCode + "\t" + responsecode;

						context.write(new Text(reducekey), new IntWritable(1));
					

				} catch (JSONException e) {
					e.printStackTrace();
				}
			}

		}
	}

	public static class NspAbilityBusdetailLogReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
