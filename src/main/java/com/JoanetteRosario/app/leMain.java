package com.JoanetteRosario.app;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class leMain{
    public static void main(String[] args) throws Exception{
        //Configuration c=new Configuration();
        //String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path("/user/joanette_rosario/raw_tweet100k.json");
        Path output=new Path("/user/joanette_rosario/tweets/output.txt");
        Job j=new Job();
        j.setJarByClass(leMain.class);
        j.setMapperClass(com.JoanetteRosario.app.twitter_mapper.class);
        j.setReducerClass(com.JoanetteRosario.app.twitter_reducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true)?0:1);
    }

}