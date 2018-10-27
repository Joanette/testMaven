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
                int id = (Integer)json.get("id");
                for(int j = 0; j<k; j++){
                    if(words[i].contains("MAGA")){
                        word.set("MAGA");
                        context.write(new Text("MAGA"), new IntWritable(id));
                    }
                    if(words[i].contains("Dictator") || words[i].contains("dictator")) {
                         word.set("Dictator");
                         context.write(new Text("Dictator"), new IntWritable(id));
                    }
                    if(words[i].contains("Impeach") || words[i].contains("impeach")){
                         word.set("Impeach");
                         context.write(new Text("Impeach"), new IntWritable(id));
                    }
                    if(words[i].contains("Drain ") || words[i].contains("Drain") ){
                        word.set("Drain");
                        context.write(new Text("Drain"), new IntWritable(id));
                    }
                    if(words[i].contains("Swamp") || words[i].contains("swamp")){
                        word.set("Swamp");
                        context.write(new Text("Swamp"), new IntWritable(id));
                    }
                    if(words[i].contains("Change") || words[i].contains("change")){
                        word.set("Change");
                        context.write(new Text("Change"),  new IntWritable(id));
                    }
                   // if (words[i].contains("Trump")||words[i].contains("trump")) {
                      //  word.set("Trump");
                        //context.write(word, one);
                   // }
                   // if(words[i].contains("Flu") || words[i].contains("flu") ){
                      //  word.set("Flu");
                      //  context.write(word, one);
                    //}
                    //if(words[i].contains("Zika") || words[i].contains("zika") ){
                      //  word.set("Zika");
                      //  context.write(word, one);
                   // }
                   // if(words[i].contains("Diarrhea") || words[i].contains("diarrhea") ){
                      //  word.set("Diarrhea");
                       // context.write(word, one);
                   // }
                   // if(words[i].contains("Ebola")  || words[i].contains("ebola") ){
                       // word.set("Ebola");
                       // context.write(word, one);
                    //}
                   // if(words[i].contains("Headache")  || words[i].contains("headache")){
                     //   word.set("Headache");
                       // context.write(word, one);
                    //}
                    //if(words[i].contains("Measles") || words[i].contains("measles") ){
                      //  word.set("Measles");
                        //context.write(word, one);
                    //}
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
