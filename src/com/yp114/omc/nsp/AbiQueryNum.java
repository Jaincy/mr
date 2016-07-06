package com.yp114.omc.nsp;

import java.io.IOException;
import java.util.Map;


import com.yp114.omc.apponofflog.BaseMr;
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

public class AbiQueryNum extends Configured implements
        Tool {

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "AbiQueryNum");
        job.setJarByClass(AbiQueryNum.class);


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
            ToolRunner.run(new AbiQueryNum(), args);
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
                           Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString().trim().replaceAll("\t", "");
            if (line.length() <= 50)
                return;
            BaseMr baseMr = new BaseMr(line);
            Map subMap = baseMr.getSubMap();
            String day_id = subMap.get("day_id").toString();
            String hour_id = subMap.get("hour_id").toString();
            String requestIp = baseMr.sub("requestip");
            String cityCode = subMap.get("cityCode").toString();
            String cityName = baseMr.sub("city_name");
            String regionCode = subMap.get("regionCode").toString();
            String latncode = subMap.get("latnCode").toString();
            String requesttype = subMap.get("requesttype").toString();
            String channelno = subMap.get("channelno").toString();
            String responsecode = baseMr.sub("responsecode");
            String imei = subMap.get("imei").toString();
            String code = baseMr.sub("code");
            String typecode1 = baseMr.sub("typecode1");
            String typecode2 = baseMr.sub("typecode2");
            // 组装
            String keyValue = day_id + "\t" + hour_id + "\t" + requestIp + "\t" + cityCode + "\t" + cityName + "\t" +
                    regionCode + "\t" + latncode + "\t" + requesttype + "\t" + channelno + "\t" + responsecode + "\t" +
                    imei + "\t" + code + "\t" + typecode1 + "\t" + typecode2 ;

            word.set(keyValue);
            context.write(word, one);
        }
    }

    public static class OnOffLog4HiveReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context)
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
