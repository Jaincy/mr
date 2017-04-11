package com.yp114.omc.apponofflog;

import com.yp114.omc.ip.IpSearch;
import com.yp114.omc.utils.LatnUtil;
import com.yp114.omc.utils.RegionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/*
 *    2015-12-06 00:00:00 {"areacode":"河北省秦皇岛市","datatime":"151702","logid":"201512060000001368283475","requestinfo":"{\"systemV
ersion\":\"4.4.4\",\"channelno\":\"1017\",\"deviceType\":\"HUAWEI C199s\",\"padOrPhone\":\"phone\",\"imei\":\"A000005508C038\",\"ent
ertime\":\"2015-12-03 08:23:32\",\"imsi\":\"460031300434886\",\"type\":\"0\",\"version\":\"7.3.4.1ctch1\"}","requestip":"123.181.202
.233","responseinfo":"{\"errorCode\":\"000000\",\"flag\":true,\"id\":0,\"msg\":\"操作成功\"}"}
   2015-12-06 00:00:00 {"areacode":"重庆市","datatime":"608317","logid":"201512060000001872232042","requestinfo":"{\"data\":[{\"ente
rtime\":\"2015-12-06 12:08:50\",\"imsi\":\"460031541474303\",\"type\":\"3\",\"channelno\":\"1210\",\"queryNum\":\"\",\"version\":\"7
.2.1.0ctch1\",\"imei\":\"A0000044C31373\"}],\"entertime\":\"2015-12-06 12:08:50\",\"imsi\":\"460031541474303\",\"type\":\"3\",\"chan
nelno\":\"1210\",\"version\":\"7.2.1.0ctch1\",\"imei\":\"A0000044C31373\"}","requestip":"14.108.14.61","responseinfo":"{\"errorCode\
":\"000000\",\"flag\":true,\"id\":0,\"msg\":\"操作成功\"}"}
 * 
 * 
 * */
public class App4HiveQueryNum extends Configured implements
		Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "NspAppOnOffLog4HiveProcessor");
		job.setJarByClass(App4HiveQueryNum.class);

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		job.setMapperClass(OnOffLog4HiveMapper.class);
		job.setCombinerClass(OnOffLog4HiveReducer.class);
		job.setReducerClass(OnOffLog4HiveReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new App4HiveQueryNum(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static class OnOffLog4HiveMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();


		@Override
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString().trim().replaceAll("\t", "").replace(" ","");



			if (line.contains("logid")&&line.length() > 15) {

				BaseMr baseMr=new BaseMr(line);

				// date_time
				// String date_time = line.substring(11, 13);
				String hour_id="noHour";
				String day_id="noDay_id";
				String time="notime";
				try {
					int timeStartIndex = line.indexOf("\"logid\":\"");
					int timeEndIndex = line.substring(
							timeStartIndex + 9).indexOf("\"");
					if (timeStartIndex > 0 && timeEndIndex > 0) {
						time = line.substring(timeStartIndex + 9,
								timeStartIndex + 9 + timeEndIndex);
						day_id=time.substring(0,8);
						hour_id=time.substring(8,10);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					InputSplit inputSplit = (InputSplit) context
							.getInputSplit();
					String filename = ((FileSplit) inputSplit).getPath()
							.getName();
					System.out.println("出错的文件名:" + filename);
					System.out.println("出错的行:" + line);
					e.printStackTrace();

				}


				/*String date_time = line.substring(0, 13);
				String[] split = date_time.split(" ");
				if (date_time.contains("201")) {
				hour_id = split[split.length - 1].substring(0,2);
				day_id = split[split.length - 2].replaceAll("-", "");
				}*/

				// areacode
				/*String areacode = "noAreacode";
				try {
					int areacodeStartIndex = line.indexOf("\"areacode\":\"");
					int areacodeEndIndex = line.substring(
							areacodeStartIndex + 12).indexOf("\"");
					if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
						areacode = line.substring(areacodeStartIndex + 12,
								areacodeStartIndex + 12 + areacodeEndIndex);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					InputSplit inputSplit = (InputSplit) context
							.getInputSplit();
					String filename = ((FileSplit) inputSplit).getPath()
							.getName();
					System.out.println("出错的文件名:" + filename);
					System.out.println("出错的行:" + line);
					e.printStackTrace();

				}

				if (areacode.length() > 2) {
					areacode = areacode.substring(0, 2);
				}

				String regionCode = RegionUtil.getRegionCode(areacode);*/

				// version

				String version =baseMr.sub("version");

				// channelno
				String channelno = baseMr.sub("channelno");



				// type 3是来显

				String type=baseMr.sub("type");
				if (type.equals("notype")){
					type="X";
				}

				/*
				 * if("3".equals(type)){ type = "LX"; }else{ type = "ZD"; }
				 */

				// IMEI
				String IMEI = baseMr.sub("imei");


				// TEL 'queryNum' : 'Optional(\"15309927986\")',
//				String TEL = baseMr.sub("queryNum").replace("\\","");
//				if (TEL.contains("Optional")&&TEL.indexOf("\")")>10){
//					TEL=TEL.substring(10,TEL.indexOf("\")"));
////					'queryNum' : 'Optional(\"15309927986\")', 've
//				}
				String TEL="noqueryNum";
				if(line.contains("queryNum")){
					int start = line.indexOf("queryNum");
					int end = line.substring(start).indexOf(",");
					TEL = line.substring(start).substring(0, end).replaceAll("[^\\d]", "");
				}

				// IMSI
				String IMSI = baseMr.sub("imsi");

				// opt
				String opt = "noCT";

				// 133/153/180/181/189/177
				if (TEL.length() == 11
						&& (TEL.startsWith("177") || TEL.startsWith("181")
								|| TEL.startsWith("133")
								|| TEL.startsWith("153")
								|| TEL.startsWith("180") || TEL
									.startsWith("189"))) {
					opt = "CT";
				}
				if (TEL.length() == 11
						&& (TEL.startsWith("130") || TEL.startsWith("131")
								|| TEL.startsWith("186")
								|| TEL.startsWith("145")
								|| TEL.startsWith("132")
								|| TEL.startsWith("176")
								|| TEL.startsWith("155")
								|| TEL.startsWith("156") || TEL
									.startsWith(" 185"))) {
					opt = "LT";
				}
				// 139 138 137 136 135 134
				// 147 150 151 152 157 158 159 178 182 183 184 187 188
				if (TEL.length() == 11
						&& (TEL.startsWith("139") || TEL.startsWith("138")
								|| TEL.startsWith("137")
								|| TEL.startsWith("136")
								|| TEL.startsWith("135")
								|| TEL.startsWith("134")
								|| TEL.startsWith("147")
								|| TEL.startsWith("150")
								|| TEL.startsWith("151")
								|| TEL.startsWith("152")
								|| TEL.startsWith("158")
								|| TEL.startsWith("159")
								|| TEL.startsWith("178")
								|| TEL.startsWith("182")
								|| TEL.startsWith("183")
								|| TEL.startsWith("184")
								|| TEL.startsWith("187")
								|| TEL.startsWith("188")
								|| TEL.startsWith("157") || TEL
									.startsWith(" 185"))) {
					opt = "YD";
				}

				if (opt.equals("noCT")) {
					// type 3是来显


					try {


						String sub="55";
                        if (IMEI.length()>5) {
                        	sub = IMSI.substring(3, 5);
						}

						if (sub.equals("00") || sub.equals("02")
								|| sub.equals("07")) {
							opt = "YD";
						}
						if (sub.equals("01") || sub.equals("06")) {
							opt = "LT";
						}
						if (sub.equals("03") || sub.equals("05")
								|| sub.equals("11")) {
							opt = "CT";
						}
						if (sub.equals("20")) {
							opt = "TT";
						}

					} catch (Exception e) {
						InputSplit inputSplit = (InputSplit) context
								.getInputSplit();
						String filename = ((FileSplit) inputSplit).getPath()
								.getName();
						System.out.println("出错的文件名:" + filename);
						System.out.println("出错的行:" + line);
						e.printStackTrace();
					}

				}



				// requestip  "requestip":"14.108.14.61"
				String requestIp = "noIp";
				String city = "noCity";
				String province = "noProvince";
				String conty="noconty";
				String cityCode="noCityCode";
				String regionCode ="noRegionCode";


				List<String> zcityList=new ArrayList<String>();
				zcityList.add("澳门");
				zcityList.add("台湾");
				zcityList.add("香港");
				zcityList.add("海南");
				zcityList.add("天津");
				zcityList.add("北京");
				zcityList.add("重庆");
				List<String> zcontyList=new ArrayList<String>();
				zcontyList.add("济源");
				zcontyList.add("仙桃");
				zcontyList.add("潜江");
				zcontyList.add("天门");
				zcontyList.add("神农架");
				zcontyList.add("阿拉尔");
				zcontyList.add("石河子");
				String[] zcity={"台湾","香港","澳门","海南","天津","北京","重庆"};
				String[] zconty={"济源","仙桃","潜江","天门","神农架","阿拉尔","石河子"};

     			try {
					int ipStartIndex = line.indexOf("\"requestip\":\"");

					int ipEndIndex = line.substring(ipStartIndex + 13).indexOf(
							"\"");
					if (ipStartIndex > 0 && ipEndIndex > 0) {
						requestIp = line.substring(ipStartIndex + 13,
								ipStartIndex + 13 + ipEndIndex);
						IpSearch finder = IpSearch.getInstance();

						String result = finder.Get(requestIp);
						//System.out.println(requestIp + ":" + result);

						result = result.toString().trim().replaceAll("\t", "");
						String[] splits = result.split("\\|");

						if (splits.length == 11) {
							province = splits[2];
							city = splits[3];
							conty=splits[4];
							if (zcityList.contains(province)) {
								city=province;
							}else if (zcontyList.contains(conty)) {
								city=conty;
							}else if ("".equals(city)) {
								city=province;
							}

							if (province.length() > 2) {
								province = province.substring(0, 2);
							}

							cityCode=LatnUtil.getRegionCode(city);
							regionCode=RegionUtil.getRegionCode(province);
						}
					}
				} catch (Exception e) {
					InputSplit inputSplit = (InputSplit) context
							.getInputSplit();
					String filename = ((FileSplit) inputSplit).getPath()
							.getName();
					System.out.println("出错的文件名:" + filename);
					System.out.println("出错的行:" + line);
					e.printStackTrace();
				}

				// ,\"deviceType\":\"HUAWEI C8817E\",\"padOrPhone\":\"phone\",\"
				String deviceType = baseMr.sub("deviceType");
				/*String deviceType="nodeviceType";
				if(line.contains("deviceType")){
					int start = line.indexOf("deviceType");
					int end = line.substring(start).indexOf(",");
					deviceType = line.substring(start).substring(0, end).replace("\"", "").replace("'","");
					deviceType=deviceType.substring(deviceType.indexOf(":")+1,deviceType.indexOf(","));
				}*/
				// ,\"padOrPhone\":\"phone\",\"
				String padOrPhone = baseMr.sub("padOrPhone");


				// 组装
				String keyValue = day_id + "\t" + hour_id + "\t" + regionCode
						+ "\t" + version + "\t" + opt + "\t" + channelno + "\t"
						+ type + "\t" + IMEI + "\t" + IMSI + "\t" +cityCode+"\t"+ requestIp
						+"\t"+ deviceType+ "\t" + padOrPhone+"\t"+TEL;
				/*
				 * String keyValue = regionCode + "#v#" + version + "#o#" + opt
				 * + "#c#" + channelno + "#t#" + type + "#i#" + IMEI;
				 */
				word.set(keyValue);
				// word.set(regionCode + "#a#" + areacode + "#v#" + version +
				// "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI
				// );
				context.write(word, one);
				/*"requestinfo":"{'systemVersi
				on':'10.0.2','channelno':'101','deviceType' : 'iPhone','padOrPhone' : 'iOS', 'imei' : 'DA1B6357-B395-435E-BDAB-E2C465F65720', 'id' :
				'', 'entertime' : '2016-10-18 12:00:21','imsi' : 'ZAQ!mko0', 'type' : '0', 'queryNum' : 'Optional(\"15309927986\")', 'version': '1
				0.2.1', 'source' : 'besttone' }"*/


			}
		}
	}

	public static class OnOffLog4HiveReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

}
