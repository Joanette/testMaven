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
        JSONParser parser = new JSONParser();
        JSONObject json;
        String text = "";
        for (int i = 0; i < tuple.length; i++) {
            try {
                json = (JSONObject) parser.parse(tuple[i]);
                //text = (String) json.get("text");
                //System.out.println(text);
                String words[] = text.split(" ");
                int k = words.length;
                long id2 = (Long) json.get("id");
                int id = (int)(long)id2;
                for (int j = 0; j < k; j++) {
                    if (tuple[i].contains("MAGA") && tuple[i].contains("maga")) {
                        context.write(new Text("MAGA"), new IntWritable(id));
                    }
                    if (tuple[i].contains("Dictator") && tuple[i].contains("dictator")) {
                        context.write(new Text("Dictator"), new IntWritable(id));
                    }
                    if (tuple[i].contains("Drain") && tuple[i].contains("drain")) {
                        context.write(new Text("Drain"), new IntWritable(id));

                    }
                    if (tuple[i].contains("Swamp") && tuple[i].contains("swamp")) {
                       context.write(new Text("Swamp"), new IntWritable(id));
                    }
                    if (tuple[i].contains("Change") && tuple[i].contains("change")) {
                        context.write(new Text("Swamp"), new IntWritable(id));
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
//        String magaColle = magaCollection.toString();
//        String dictatorColle = dicatatorCollection.toString();
//        String drainColle = drainCollection.toString();
//        String swampColle = swampCollection.toString();
//        String changeColle = changeCollection.toString();
//        context.write(new Text(magaColle), new IntWritable(0));
//        context.write(new Text(dictatorColle), new IntWritable(0));
//        context.write(new Text(drainColle), new IntWritable(0));
//        context.write(new Text(swampColle), new IntWritable(0));
//        context.write(new Text(changeColle), new IntWritable(0));
    }
}

