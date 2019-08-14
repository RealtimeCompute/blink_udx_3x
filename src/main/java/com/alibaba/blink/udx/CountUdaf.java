package com.alibaba.blink.udx;



import org.apache.flink.table.functions.AggregateFunction;

public class CountUdaf extends AggregateFunction<Long, CountUdaf.CountAccum> {
    //定义存放count udaf的状态的accumulator数据结构
    public static class CountAccum {
        public long total;
    }

    //初始化count udaf的accumulator
    public CountAccum createAccumulator() {
        CountAccum acc = new CountAccum();
        acc.total = 0;
        return acc;
    }

    //getValue提供了如何通过存放状态的accumulator计算count UDAF的结果的方法
    public Long getValue(CountAccum accumulator) {
        return accumulator.total;
    }

    //accumulate提供了如何根据输入的数据更新count UDAF存放状态的accumulator
    public void accumulate(CountAccum accumulator, long iValue) {
        accumulator.total+=iValue;
    }

    public void merge(CountAccum accumulator, Iterable<CountAccum> its) {
        for (CountAccum other : its) {
            accumulator.total += other.total;
        }
    }
}
