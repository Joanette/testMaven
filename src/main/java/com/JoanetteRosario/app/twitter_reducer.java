package com.JoanetteRosario.app;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class twitter_reducer  extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        Iterator<IntWritable> valuesIt = values.iterator();
        while (valuesIt.hasNext()) {
            sum = sum + valuesIt.next().get();
        }
        System.out.println("this be the sum whutup "+sum+"\n" );
        context.write(key, new IntWritable(sum));
    }
}