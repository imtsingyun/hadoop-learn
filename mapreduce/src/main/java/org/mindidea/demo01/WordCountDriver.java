/*
 * @class WordCountDriver
 * @package org.mindidea.demo01_wordcount
 * @date 2021/1/10 16:39
 * Copyright (c) 2021 MindIdea.org, All Rights Reserved.
 */
package org.mindidea.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Tsingyun(青雲)
 * @version V1.0
 * @createTime 2021/1/10 16:39
 * @blog https://mindidea.org
 */
public class WordCountDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"e:/input", "e:/output"};
        Configuration conf = new Configuration();
        // 1. 获取 job 对象
        Job job = Job.getInstance(conf);
        // 2. 设置 jar 存储位置
        job.setJarByClass(WordCountMapper.class);
        // 3. 关联 map 和 reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        // 4. 设置Mapper 输出 数据的key和value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5. 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7. 提交 job
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
