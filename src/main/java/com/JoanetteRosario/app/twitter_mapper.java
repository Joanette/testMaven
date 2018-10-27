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


public class twitter_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tuple = line.split("\\n");
        String s = "";
        JSONParser parser = new JSONParser();
        try {
            for (int i = 0; i < tuple.length; i++) {
                JSONObject json = (JSONObject) parser.parse(tuple[i]);
                String text = (String) json.get("text");
                String words[]= text.split(" ");
                int k = words.length;
                for(int j = 0; j<k; j++){
                    if (words[i].contains("Trump")||words[i].contains("trump")) {
                        word.set("Trump");
                        context.write(word, one);
                    }
                    if(words[i].contains("Flu") || words[i].contains("flu") ){
                        word.set("Flu");
                        context.write(word, one);
                    }
                    if(words[i].contains("Zika") || words[i].contains("zika") ){
                        word.set("Zika");
                        context.write(word, one);
                    }
                    if(words[i].contains("Diarrhea") || words[i].contains("diarrhea") ){
                        word.set("Diarrhea");
                        context.write(word, one);
                    }
                    if(words[i].contains("Ebola")  || words[i].contains("ebola") ){
                        word.set("Ebola");
                        context.write(word, one);
                    }
                    if(words[i].contains("Headache")  || words[i].contains("headache")){
                        word.set("Headache");
                        context.write(word, one);
                    }
                    if(words[i].contains("Measles") || words[i].contains("measles") ){
                        word.set("Measles");
                        context.write(word, one);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
