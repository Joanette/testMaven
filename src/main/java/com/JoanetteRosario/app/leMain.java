package com.JoanetteRosario.app;

import java.io.IOException;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class leMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountNamesByState <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(com.JoanetteRosario.app.leMain.class);
        job.setJobName("Tweets");
        args[0] = "/user/joanette_rosario/tweets/raw_tweet100k.json";
        args[1] = "/user/joanette_rosario/tweets/output.txt";

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(com.JoanetteRosario.app.twitter_mapper.class);
        job.setReducerClass(com.JoanetteRosario.app.twitter_reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("job.isSuccessful " + job.isSuccessful());
    }
}
