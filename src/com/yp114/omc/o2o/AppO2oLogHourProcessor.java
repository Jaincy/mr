package com.yp114.omc.o2o;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class AppO2oLogHourProcessor{
        /*
		 *
		 * AppO2O  小时日志统计，分channlno和id纬度
		 */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[]otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("用法：AppO2oLogHourProcessor <in> <out>");
            System.exit(2);
        }

//        // 启用输出文件压缩
//        conf.setBoolean("mapred.output.compress", true);
//        // // 选择压缩器
//        conf.setClass("mapred.output.compression.codec", BZip2Codec.class,
//                CompressionCodec.class);

        Job job = Job.getInstance(conf, "AppO2oLogHourProcessor");

        job.setJarByClass(AppO2oLogHourProcessor.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AppO2oLogHourMapper.class);
        job.setCombinerClass(AppO2oLogHourReducer.class);
        job.setReducerClass(AppO2oLogHourReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class AppO2oLogHourMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text channelnoWord = new Text();
        private Text idWord = new Text();

        @Override
        protected void map(Object key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            line = StringUtils.replaceBlank(line);
            if (line.length() > 0) {
                // ID
                String ID;
                int idStartIndex = line.indexOf("\\\"ID\\\":\\\"");
                int idEndIndex = line.substring(idStartIndex + 9).indexOf(
                        "\\\"");
                if (idStartIndex > 0 && idEndIndex > 0) {
                    ID = line.substring(idStartIndex + 9, idStartIndex + 9
                            + idEndIndex);
                } else {
                    ID = "null";
                }

                // channelno
                String channelno = "noChannel";
                int channelnoStartIndex = line.indexOf("channelno\\\":\\\"");
                int channelnoEndIndex = line
                        .substring(channelnoStartIndex + 14).indexOf("\\\"");
                if (channelnoStartIndex > 0 && channelnoEndIndex > 0) {
                    channelno = line.substring(channelnoStartIndex + 14,
                            channelnoStartIndex + 14 + channelnoEndIndex);
                }

                // imei
                String imei = "noImei";
                int imeiStartIndex = line.indexOf("imei\\\":\\\"");
                int imeiEndIndex = line.substring(imeiStartIndex + 9).indexOf(
                        "\\\"");
                if (imeiStartIndex > 0 && imeiEndIndex > 0) {
                    imei = line.substring(imeiStartIndex + 9, imeiStartIndex
                            + 9 + imeiEndIndex);
                }

                // 组装
                String c = "\t"; // 分隔符
                channelnoWord.set("channel"+c+channelno + c + imei);
                idWord.set("id"+c+ID + c + imei);
                context.write(channelnoWord, one);
                context.write(idWord, one);
            }

        }
    }

    public static class AppO2oLogHourReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
