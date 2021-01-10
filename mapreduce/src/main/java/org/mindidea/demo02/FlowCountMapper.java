/*
 * @class FlowMapper
 * @package org.mindidea.demo02
 * @date 2021/1/10 22:18
 * Copyright (c) 2021 MindIdea.org, All Rights Reserved.
 */
package org.mindidea.demo02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Tsingyun(青雲)
 * @version V1.0
 * @createTime 2021/1/10 22:18
 * @blog https://mindidea.org
 */
// extends Mapper<读取内容的偏移量(key), 读取的一行的内容, 输出内容的key, 输出的值value>
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        k.set(fields[1]);
        Long upFlow = Long.parseLong(fields[fields.length - 3]);
        Long downFlow = Long.parseLong(fields[fields.length - 2]);
        v.set(upFlow, downFlow);

        context.write(k, v);
    }
}
