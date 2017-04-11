package com.yp114.omc.apponofflog;

import com.yp114.omc.ip.IpSearch;
import com.yp114.omc.utils.LatnUtil;
import com.yp114.omc.utils.RegionUtil;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;
import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Object;
import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.newObjectType;


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
public class AppLoyalty extends Configured implements
		Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "AppLoyalty");
		job.setJarByClass(AppLoyalty.class);

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
			ToolRunner.run(new AppLoyalty(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static class OnOffLog4HiveMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		private final static LongWritable one = new LongWritable();
		private Text word = new Text();


		@Override
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString().trim().replaceAll("\t", "");

			if (line.contains("logid")&&line.length() > 15) {

				BaseMr baseMr = new BaseMr(line);
				Map subMap = baseMr.getSubMap();

				String day_id = subMap.get("day_id").toString();
				if (day_id.equals("noday_id"))
					return;
				String hour_id = subMap.get("hour_id").toString();
				String min_id=subMap.get("min_id").toString();
				String sec_id=subMap.get("sec_id").toString();
				String imei = subMap.get("imei").toString();

				String time=day_id+hour_id+min_id;
				DateFormat fmt =new SimpleDateFormat("yyyyMMdd");
				Date date = null;
				try {
					date = fmt.parse(time);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				long seccnt=date.getTime();
//				int seccnt=Integer.parseInt(hour_id)*60*60+Integer.parseInt(min_id)*60+Integer.parseInt(sec_id);
				// 组
//				String keyValue = imei+"\t"+day_id+"\t"+hour_id+"\t"+min_id+"\t"+seccnt;
				String keyValue=imei;

				word.set(keyValue);

				one.set(seccnt);
				context.write(word, one);

			}
		}
	}

	public static class OnOffLog4HiveReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		private Text word = new Text();


		protected void reduce(Text key, Iterable<LongWritable> values,
							  Reducer<Text, IntWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			/*int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);*/
			HashSet h=new HashSet();
			List l=new ArrayList();
            String s="";
			for (LongWritable val : values
				 ) {
				h.add(val.get());
				s=val.toString()+","+val.get()+","+s;

			}
//			String[] ia=new String[h.size()];
			Object[] o=h.toArray();
			Arrays.sort(o);
			String k="";
			for (int i = 0; i <o.length ; i++) {
				k+=","+o[i];
			}
			word.set(s);
			result.set(o.length);
			context.write(word,result);
		}
	}
}
