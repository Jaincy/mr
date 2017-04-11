package com.yp114.omc.o2o;

import java.io.IOException;
import java.util.Map;

import com.yp114.omc.apponofflog.BaseMr;
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

public class AppO2oHour extends Configured implements Tool {

    public static void main(String[] args) {
		/*
		 * 
		 * AppO2o日志 第一步 日志清洗
		 */

        try {
            ToolRunner.run(new Configuration(), new AppO2oHour(),
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

        Job job = Job.getInstance(conf, "AppO2oHour");

        job.setJarByClass(AppO2oHour.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AppO2oHourMapper.class);
        job.setCombinerClass(AppO2oHourReducer.class);
        job.setReducerClass(AppO2oHourReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AppO2oHourMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            line = StringUtils.replaceBlank(line);

            /* 2016-12-12 00:01:21 {"areacode":"河南省郑州市","logid":"201612120001201725577261","requestinfo":"{\"ID\":\"bohaobohao\",\"NAME\":\"拨号\",\"NOTE\":\"拨号\",\"channelno\":\"1006\",\"cityCode
                \":\"000000\",\"cityName\":\"全国\",\"imei\":\"a0000055b48827\",\"queryNum\":\"\"}","requestip":"171.15.185.38","responseinfo":"{\"errorCode\":\"000000\",\"flag\":true,\"id\":0}","userAgent":
                "Dalvik/2.1.0 (Linux; U; Android 5.0; PLK-CL00 Build/HONORPLK-CL00)"}*/

            if (line.length() > 0) {
                BaseMr baseMr=new BaseMr(line);
                Map subMap = baseMr.getSubMap();
                String day_id = subMap.get("day_id").toString();
                String hour_id = subMap.get("hour_id").toString();

                //System.out.println(logid);

                // areacode
                String areacode = baseMr.sub("areacode");

                String ID = baseMr.sub("ID");


                // name
                String name = baseMr.sub("NAME");

                // channelno
                String channelno = baseMr.sub("channelno");

                // imei
                String imei = baseMr.sub("imei");

                String querynum=baseMr.sub("queryNum");

                // 组装
                String c = "\t"; // 分隔符
                String str = day_id + c + hour_id + c + ID + c + name + c
                        + channelno + c + imei + c +querynum+c+ areacode;
                word.set(str);

                context.write(word, one);

            }

        }
    }

    public static class AppO2oHourReducer extends
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
