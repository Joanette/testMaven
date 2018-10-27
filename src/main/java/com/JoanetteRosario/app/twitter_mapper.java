package com.JoanetteRosario.app;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class twitter_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tuple = line.split("\\n");
        String s = "";
        JSONParser parser = new JSONParser();
        JSONObject json = null;
        try {
            for (int i = 0; i < tuple.length; i++) {
                String text = (String) json.get("text");
                String words[]= text.split(" ");
                int k = words.length;
                int id = (Integer)json.get("id");
                for(int j = 0; j<k; j++){
                    if(words[i].contains("FLU")){
                        word.set("MAGA");
                        context.write(new Text("MAGA"), new IntWritable(1));
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
