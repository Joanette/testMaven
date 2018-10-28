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

public class leMain {

    public static void main(String[] args) throws Exception{
        //int exitCode = ToolRunner.run(new leMain(), args);

        //public int run(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(leMain.class);
        job.setJobName("WordCounter");
        args[0] = "/user/joanette_rosario/tweets/raw_tweet100k.json";
        args[1] = "/user/joanette_rosario/tweets/output.txt";
        System.out.println("args[0] " + args[0]);
        System.out.println("args[1]"+args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(twitter_mapper.class);
        job.setReducerClass(twitter_reducer.class);

        System.out.println("job.isSuccessful " + job.isSuccessful());
        System.exit(job.waitForCompletion(true)? 0 :1 );
    }
}
