package com.yp114.omc.apponofflog.retained;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

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

public class AppOnTime2 extends Configured implements
        Tool {

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "AppOnTime2");
        job.setJarByClass(AppOnTime2.class);

        FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        job.setMapperClass(OnOffLog4HiveMapper.class);
        job.setCombinerClass(OnOffLog4HiveReducer.class);
        job.setReducerClass(OnOffLog4HiveReducer.class);
        // job.setPartitionerClass(cls);
        // job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new AppOnTime2(), args);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static class OnOffLog4HiveMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        java.util.Date d1;
        java.util.Date d2;

        long diff = 0;
        String LastImei = "xxxxxxxxxx";
        String LastTime = "20160101000000";
        long diff_secondes = 0;
        int count = 1;

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            String[] split = value.toString().split("\t");
            String imei = split[0];
            String s2 = split[1];

            if (imei.equals(LastImei)) {
                try {
                    d1 = df.parse(LastTime);
                    d2 = df.parse(s2);

                    diff = (d2.getTime() - d1.getTime()) / 1000;
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                diff_secondes = (diff > 600) ? 0 : diff;

                String keyValue = imei + "\t" + LastTime + "\t" + s2 + "\t"
                        + diff_secondes;
                word.set(keyValue);
                context.write(word, one);

            } else {

                String keyValue = imei + "\t" + s2 + "\t" + s2 + "\t" + 0;
                word.set(keyValue);
                context.write(word, one);

            }

            LastImei = imei;
            LastTime = s2;

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
