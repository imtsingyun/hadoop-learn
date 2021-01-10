/*
 * @class WordCountReduce
 * @package org.mindidea.demo01_wordcount
 * @date 2021/1/10 15:26
 * Copyright (c) 2021 MindIdea.org, All Rights Reserved.
 */
package org.mindidea.demo01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *
 * @author Tsingyun(青雲)
 * @version V1.0
 * @createTime 2021/1/10 15:26
 * @blog https://mindidea.org
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // 1. 累加求和
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
        }
        v.set(sum);
        // 2. 写出
        context.write(key, v);
    }
}
