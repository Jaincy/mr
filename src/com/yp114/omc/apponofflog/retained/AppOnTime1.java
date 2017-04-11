package com.yp114.omc.apponofflog.retained;

import java.io.IOException;
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
public class AppOnTime1 extends Configured implements
        Tool {

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "AppOnTime1");
        job.setJarByClass(AppOnTime1.class);

        FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        job.setMapperClass(OnOffLog4HiveMapper.class);
        job.setCombinerClass(OnOffLog4HiveReducer.class);
        job.setReducerClass(OnOffLog4HiveReducer.class);
        //job.setPartitionerClass(cls);
        //设置reduce作业为一个，最后全局排序
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new AppOnTime1(), args);
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

            if (line.length() > 0) {

                // date_time
                // String date_time = line.substring(11, 13); 
                //"logid":"2016 06 19 00 01 42 1420499624	
                String date_time = "0";


                int logidStart  =  line.indexOf("\"logid\":\"");
                int logidEnd=0;
                if(logidStart>0)
                    logidEnd  =  line.substring(logidStart+9).indexOf("\"");
                if(logidStart>0 && logidEnd>0)
                    date_time	= line.substring(logidStart+9,logidStart+9+14);

                // type 3是来显
                String type = "X";


                int typeStartIndex = line.indexOf("\\\"type\\\":\\\"");
                // int typeStartIndex =
                // line.indexOf("\\\"callstype\\\":\\\"");
                int typeEndIndex=0;
                if(typeStartIndex>0)
                    typeEndIndex = line.substring(typeStartIndex + 11)
                            .indexOf("\\\"");
                if (typeStartIndex > 0 && typeEndIndex > 0) {
                    type = line.substring(typeStartIndex + 11,
                            typeStartIndex + 11 + typeEndIndex);
                }



                // IMEI
                String IMEI = "noIMEI";

                int imeiStartIndex = line.indexOf("\\\"imei\\\":\\\"");
                int imeiEndIndex=0;
                if(imeiStartIndex>0)
                    imeiEndIndex= line.substring(imeiStartIndex + 11)
                            .indexOf("\\\"");
                if (imeiStartIndex > 0 && imeiEndIndex > 0) {
                    IMEI = line.substring(imeiStartIndex + 11,
                            imeiStartIndex + 11 + imeiEndIndex);
                }


                if(!IMEI.equals("noIMEI") && !type.equals("X") && !date_time.equals("0") &&  ( "0".equals(type) || "2".equals(type)  ) && !"000000000000000".equals(IMEI))
                {
                    //String keyValue = IMEI + "\t" + date_time + "\t" ;
                    String keyValue = IMEI + "\t" + date_time   ;

                    word.set(keyValue);
                    context.write(word, one);
                }

            }
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
