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

public class leMain extends Configured implements Tool{

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new leMain(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        // if (args.length != 2) {
        // System.err.printf("Usage: %s [generic options] <input> <output>\n",
        //  getClass().getSimpleName());
        // ToolRunner.printGenericCommandUsage(System.err);
        // return -1;
        //}

        Job job = new org.apache.hadoop.mapreduce.Job();
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
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(twitter_mapper.class);
        job.setReducerClass(twitter_reducer.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;
        System.out.println("job.isSuccessful " + job.isSuccessful());
        return returnValue;
    }
}
