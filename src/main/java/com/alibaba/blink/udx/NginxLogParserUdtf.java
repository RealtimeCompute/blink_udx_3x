package com.alibaba.blink.udx;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.types.Row;


/*
    以如下配置文件中定义的Nginx日志打印格式，即main格式为例：
    log_format main  '$remote_addr - $remote_user [$time_local] "$request" '
                 '$request_time $request_length '
                 '$status $body_bytes_sent "$http_referer" '
                 '"$http_user_agent"';
    日志样例：192.168.1.2 - UserA [10/Jul/2015:15:51:09 +0800] "GET /ubuntu.iso HTTP/1.0" 0.000 129 404 168 "-" "Wget/1.11.4 Red Hat modified"
    返回结果：
    row(0) : 192.168.1.2
    row(1) : UserA
    row(2) : 10/Jul/2015:15:51:09 +0800
    row(3) : GET /ubuntu.iso HTTP/1.0
    row(4) : 0.0
    row(5) : 129
    row(6) : 404
    row(7) : 168
    row(8) : -
    row(9) : Wget/1.11.4 Red Hat modified
*/


public class NginxLogParserUdtf extends TableFunction<Row> {

    // 可选， open方法可以不写,若写需要import org.apache.flink.table.functions.FunctionContext;
    @Override
    public void open(FunctionContext context) {
        // ... ...
    }

    public void eval(String nginxLogBody) {
        Row row = new Row(10);
        String[] strArray1 = nginxLogBody.split("\"");
        row.setField(3,strArray1[1]);
        row.setField(8,strArray1[3]);
        row.setField(9,strArray1[5]);
        String nginxLogBody1 = nginxLogBody.replace(" \""+strArray1[1]+"\"","")
                .replace(" \""+strArray1[3]+"\"","")
                .replace(" \""+strArray1[5]+"\"","");
        String[] strArray2 = nginxLogBody1.split("[\\[\\]]");
        row.setField(2,strArray2[1]);
        String nginxLogBody2 = nginxLogBody1.replace(" ["+strArray2[1]+"]","");
        String[] strArray3 = nginxLogBody2.split(" ");
        row.setField(0,strArray3[0]);
        row.setField(1,strArray3[2]);
        row.setField(4,Double.valueOf(strArray3[3]));
        row.setField(5,Long.valueOf(strArray3[4]));
        row.setField(6,strArray3[5]);
        row.setField(7,Long.valueOf(strArray3[6]));
        collect(row);
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return DataTypes.createRowType(DataTypes.STRING ,DataTypes.STRING ,DataTypes.STRING ,DataTypes.STRING ,
                DataTypes.DOUBLE ,DataTypes.LONG ,DataTypes.STRING ,DataTypes.LONG ,DataTypes.STRING, DataTypes.STRING);
    }

    // 可选，close方法可以不写
    @Override
    public void close() {
        // ... ...
    }
}
