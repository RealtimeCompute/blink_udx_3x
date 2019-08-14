package com.alibaba.blink.udx;




import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

public class SplitUdtf extends TableFunction<String> {

    // 可选， open方法可以不写,若写需要import org.apache.flink.table.functions.FunctionContext;
    @Override
    public void open(FunctionContext context) {
        // ... ...
    }

    public void eval(String str) {
        String[] split = str.split("\\|");
        for (String s : split) {
            collect(s);
        }
    }

    // 可选，close方法可以不写
    @Override
    public void close() {
        // ... ...
    }

}