package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable sum = new IntWritable();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

//        Iterator<IntWritable> iterator = values.iterator();
//
//        while (iterator.hasNext()) {
//            sum += iterator.next().get();
//        }

        for (IntWritable value : values) {
            sum += value.get();
        }

        this.sum.set(sum);
        context.write(key, this.sum);
    }
}
