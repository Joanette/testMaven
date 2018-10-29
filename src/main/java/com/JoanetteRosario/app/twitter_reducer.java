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
        StringBuilder collection = new StringBuilder();
        collection.append(key+" ");
        int sum = 0;
        Iterator<IntWritable> valuesIt = values.iterator();
        while (valuesIt.hasNext()) {
            collection.append(valuesIt.next().get()+ ", ");
        }
        context.write(new Text(collection.toString()), new IntWritable());
    }
}