package com.yp114.omc.nsp;

import com.yp114.omc.apponofflog.BaseMr;
import com.yp114.omc.utils.RegionUtil;
import org.apache.commons.lang.StringEscapeUtils;
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
import org.apache.hadoop.nfs.nfs3.request.SYMLINK3Request;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup  ;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Jain on 2016/5/30.
 */
public class AbiHuawei extends Configured implements Tool{
    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "AbiHuawei");
        job.setJarByClass(AbiHuawei.class);
        job.getJar();

        FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class) ;
        job.setMapperClass(OnOffLog4HiveMapper.class);
        job.setCombinerClass(OnOffLog4HiveReducer.class);
        job.setReducerClass(OnOffLog4HiveReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new AbiHuawei(), args);
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
            String areacode=baseMr.sub("areacode");

            if (areacode.length() > 2) {
                areacode = areacode.substring(0, 2);
            }
//            String regionCode = RegionUtil.getRegionCode(areacode);
            String regionCode=subMap.get("regionCode").toString();

            String channelno=baseMr.sub("channelno");
            String imei=baseMr.sub("imei");
            String imsi=baseMr.sub("imsi");
            String queryNum=baseMr.sub("queryNum");
            if (queryNum.equals("noqueryNum")) {
                queryNum = "noTEL";
            }
            
            String sjNum=subMap.get("subjectNum").toString();
            String requesttype = baseMr.sub("requesttype");

            if (sjNum.equals("return"))
                return;
            String responsecode = baseMr.sub("responsecode");
            String custId = baseMr.sub("custId");
            String custName = subMap.get("custName").toString();

           /* //后加四个字段
            String url="http://180.153.50.28:9080/hy114/ClassGet?tel="+sjNum;
            Document doc = Jsoup.connect(url).get();
            String body = doc.body().toString();
            String st = StringEscapeUtils.unescapeHtml((body));
            BaseMr bm = new BaseMr(st);
            String classcode1 = bm.sub("classcode1");
            String classcode2 = bm.sub("classcode2");
            String classname1 = bm.sub("classname1");
            String classname2  = bm.sub("classname2");*/


            //组装
            String keyValue =day_id + "\t" +hour_id + "\t" +regionCode + "\t" + channelno + "\t" + imei
                    + "\t" + imsi + "\t" + queryNum + "\t" + sjNum+ "\t" + requesttype+ "\t" + responsecode+ "\t"
                    + custId + "\t" + custName;;

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
