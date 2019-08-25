package com.h3c.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 词频统计示例代码
 */
public class SimpleExample {
    public SimpleExample(){}

    /***
     * main()方法创建一个job，指定参数，提交作业到hadoop集群
     */
    public static void main(String[] args) throws Exception{
        // 初始化环境变量
        Configuration conf = new Configuration();

        // 获取入参
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        // 初始化Job任务对象
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(SimpleExample.class);

        // 设置运行时执行map，reduce的类
        job.setMapperClass(SimpleExample.TokenizerMapper.class);
        // 设置combiner类，默认不使用，使用时通常使用和reduce一样的类
        job.setCombinerClass(SimpleExample.IntSumReducer.class);
        job.setReducerClass(SimpleExample.IntSumReducer.class);

        // 设置作业的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // 提交任务交到远程环境上执行
        System.exit(job.waitForCompletion(true)?0:1);
    }

    /***
     * 类IntSumReducer定义Reducer抽象类的reduce()方法
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        // 统计结果
        private IntWritable result = new IntWritable();

        public IntSumReducer(){}

        /***
         *
         * @param key Text : Mapper后的key项
         * @param values Iterable : 相同key项的所有统计结果
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
            int sum =0;
            for(IntWritable val :values){
                sum +=val.get();
            }
            result.set(sum);
            // reduce输出为key：字符串，value：字符串出现的总数
            context.write(key,result);
        }
    }

    /***
     * 类TokenizerMapper定义Mapper抽象类的map()方法
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // 输出的key,value要求是序列化的
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public TokenizerMapper(){}

        /***
         *
         * @param key Object : 原文件位置偏移量
         * @param value Text : 原文件的字符串数据
         * @param context Context : 出参
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            // 读取的一行字符串数据，按分隔符进行分割
            StringTokenizer itr = new StringTokenizer(value.toString(),",");

            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                // map输出key，value键值对
                context.write(word, one);
            }
        }
    }
}