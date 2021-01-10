/*
 * @class FlowCountReduce
 * @package org.mindidea.demo02
 * @date 2021/1/10 22:42
 * Copyright (c) 2021 MindIdea.org, All Rights Reserved.
 */
package org.mindidea.demo02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Tsingyun(青雲)
 * @version V1.0
 * @createTime 2021/1/10 22:42
 * @blog https://mindidea.org
 */
public class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean bean: values) {
            sumUpFlow += bean.getUpFlow();
            sumDownFlow += bean.getDownFlow();
        }
        v.set(sumUpFlow, sumDownFlow);

        context.write(key, v);
    }
}
