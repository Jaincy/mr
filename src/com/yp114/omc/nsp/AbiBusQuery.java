package com.yp114.omc.nsp;


import com.yp114.omc.apponofflog.BaseMr;
import com.yp114.omc.ip.IpSearch;
import com.yp114.omc.utils.LatnUtil;
import com.yp114.omc.utils.RegionUtil;
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
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 更新:从ability_querynum日志目录中过滤出商家详情信息(包含shopid等字段的)
 *
 * @author zhenzhen
 *         udpated 2016-12-04 下午2:11
 */
public class AbiBusQuery extends Configured implements Tool {

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "AbiBusQuery");
        job.setJarByClass(AbiBusQuery.class);

        FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        job.setMapperClass(AbiBusQueryMapper.class);
        job.setCombinerClass(AbiBusQueryReducer.class);
        job.setReducerClass(AbiBusQueryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {

        try {
            ToolRunner.run(new AbiBusQuery(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class AbiBusQueryMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value,
                           Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            BaseMr baseMr = new BaseMr(line);
            Map subMap = baseMr.getSubMap();
            String requesttype = subMap.get("requesttype").toString();

            if (requesttype.equals("105") || requesttype.equals("170")) {
                String day_id = subMap.get("day_id").toString();
                String hour_id = subMap.get("hour_id").toString();
                String requestip = baseMr.sub("requestip");

                String provinceCode = null;
                String latncode = null;
                if (requesttype.equals("105")) {
                    provinceCode = baseMr.sub("regionCode");
                    latncode = baseMr.sub("cityCode");
                } else {
                    provinceCode = subMap.get("regionCode").toString();
                    latncode = subMap.get("regionCode").toString();

                }

                String channelno = subMap.get("channelno").toString();
                String responsecode = baseMr.sub("responsecode");
                String custId = baseMr.sub("custId");
                String custName = baseMr.sub("custName");
                String shopid = baseMr.sub("shopid");
                String region = baseMr.sub("region");
                String shopname = baseMr.sub("shopname");
                String subjectNum = baseMr.sub("value");
                String name = baseMr.sub("name");


                String reducekey = day_id + "\t" + hour_id + "\t" + subjectNum + "\t" + region + "\t" +
                        shopid + "\t" + shopname + "\t" + channelno + "\t" + name + "\t" + requestip + "\t"
                        + latncode + "\t" + provinceCode + "\t" + responsecode + "\t" + requesttype + "\t"
                        + custId + "\t" + custName;

                context.write(new Text(reducekey), new IntWritable(1));

            }
        }
    }

    public static class AbiBusQueryReducer extends
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
