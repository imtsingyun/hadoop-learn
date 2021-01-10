/*
 * @class FlowBean
 * @package org.mindidea.demo02
 * @date 2021/1/10 21:58
 * Copyright (c) 2021 MindIdea.org, All Rights Reserved.
 */
package org.mindidea.demo02;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义对象
 *
 * @author Tsingyun(青雲)
 * @version V1.0
 * @createTime 2021/1/10 21:58
 * @blog https://mindidea.org
 */
public class FlowBean implements Writable {

    // 上行流量
    private long upFlow;

    // 下行流量
    private long downFlow;

    // 总流量
    private long sumFlow;

    // 空参构造器，后继反射用
    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    // 序列化方法
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(upFlow);
        output.writeLong(downFlow);
        output.writeLong(sumFlow);
    }

    // 反序列化方法
    @Override
    public void readFields(DataInput input) throws IOException {
        upFlow = input.readLong();
        downFlow = input.readLong();
        sumFlow = input.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void set(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }
}
