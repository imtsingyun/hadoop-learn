/*
 * @class WordCount
 * @package org.mindidea
 * @date 2021/1/10 14:50
 * Copyright (c) 2021 MindIdea.org, All Rights Reserved.
 */
package org.mindidea.demo01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN:       输入数据的 key 的类型
 * VALUEIN:     输入数据的 value 的类型
 * KEYOUT:      输出数据的 key 的类型
 * VALUEOUT:    输出数据的 value 的类型
 *
 * @author Tsingyun(青雲)
 * @version V1.0
 * @blog https://mindidea.org
 * @since 2021/1/10 14:50
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1. 获取一行
        String line = value.toString();
        // 2. 切割单词
        String[] words = line.split(" ");
        // 3. 循环写出
        for (String word: words) {
            k.set(word);
            context.write(k, v);
        }

    }
}
