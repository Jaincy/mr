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


public class AppO2oLogHourSumProcessor {
         /*
		 *
		 * AppO2O  小时日志统计，分channlno和id纬度合计
		 */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("用法：AppO2oLogHourSumProcessor <in> <out>");
            System.exit(2);
        }

//        // 启用输出文件压缩
//        conf.setBoolean("mapred.output.compress", true);
//        // // 选择压缩器
//        conf.setClass("mapred.output.compression.codec", BZip2Codec.class,
//                CompressionCodec.class);

        Job job = Job.getInstance(conf, "AppO2oLogHourSumProcessor");

        job.setJarByClass(AppO2oLogHourSumProcessor.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AppO2oLogHourSumMapper.class);
        job.setCombinerClass(AppO2oLogHourSumReducer.class);
        job.setReducerClass(AppO2oLogHourSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class AppO2oLogHourSumMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String c = "\t"; // 分隔符
            String[] split = line.split(c);
            if (split.length == 4) {
                word.set(split[0] + c + split[1]);
            }
            context.write(word, one);

        }
    }

    public static class AppO2oLogHourSumReducer extends
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
