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
        String keyToSplit = key.toString();
        StringBuilder magaCollection = new StringBuilder();
        magaCollection.append("MAGA, ");
        StringBuilder dicatatorCollection = new StringBuilder();
        dicatatorCollection.append("Dictator, ");
        StringBuilder drainCollection = new StringBuilder();
        drainCollection.append("Drain, ");
        StringBuilder swampCollection = new StringBuilder();
        swampCollection.append("Swamp, ");
        StringBuilder changeCollection = new StringBuilder();
        changeCollection.append("Change, ");

        String ids[] = keyToSplit.split(",");
        if(keyToSplit.contains("MAGA")){
            magaCollection.append(ids[1]);
        }
        if(keyToSplit.contains("Dictator")){
            dicatatorCollection.append(ids[1]);
        }
        if(keyToSplit.contains("Drain")){
            drainCollection.append(ids[1]);
        }
        if(keyToSplit.contains("Swamp")){
            swampCollection.append(ids[1]);
        }
        if(keyToSplit.contains("Change")){
            changeCollection.append(ids[1]);
        }

        context.write(new Text(magaCollection.toString()), new IntWritable(sum));
        context.write(new Text(dicatatorCollection.toString()), new IntWritable(sum));
        context.write(new Text(drainCollection.toString()), new IntWritable(sum));
        context.write(new Text(swampCollection.toString()), new IntWritable(sum));
        context.write(new Text(changeCollection.toString()), new IntWritable(sum));
    }
}