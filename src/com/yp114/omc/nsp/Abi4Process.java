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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Abi4Process extends Configured implements
        Tool {

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Abi4Process");
        job.setJarByClass(Abi4Process.class);
        //动态输入路径
        for (int i=0;i<arg0.length-1;i++)
            FileInputFormat.addInputPath(job,new Path(arg0[i]));

        FileOutputFormat.setOutputPath(job, new Path(arg0[arg0.length-1]));
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
            ToolRunner.run(new Abi4Process(), args);
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
            if (line.length() <= 50  )
                return;
            BaseMr baseMr = new BaseMr(line);
            Map subMap = baseMr.getSubMap();

            String day_id = subMap.get("day_id").toString();
            if (day_id.equals("noday_id"))
                return;
            String hour_id = subMap.get("hour_id").toString();
            String requestIp = baseMr.sub("requestip");
            String cityCode = subMap.get("cityCode").toString();
            String cityName = baseMr.sub("city_name");
            String queryNum=baseMr.sub("queryNum");
            String regionCode = subMap.get("regionCode").toString();
            String requesttype = subMap.get("requesttype").toString();
            String channelno = subMap.get("channelno").toString();
            String imsi=baseMr.sub("imsi");
            String subjectNum=subMap.get("subjectNum").toString();
            if (subjectNum.equals("return"))
                return;
            String responsecode = baseMr.sub("responsecode");
            String imei = subMap.get("imei").toString();
            String latnCode=subMap.get("latnCode").toString();
            // 组装
            String reducekey = day_id + "\t" + hour_id + "\t" + requestIp + "\t"
                    + channelno+ "\t" + imei + "\t" + imsi + "\t"
                    + cityCode + "\t" + cityName + "\t" + latnCode
                    + "\t" + regionCode+ "\t" + queryNum + "\t" + subjectNum
                    + "\t" + requesttype + "\t" + responsecode;
            context.write(new Text(reducekey), new IntWritable(1));
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
