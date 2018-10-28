package tester;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

public class main {
    public static void main(String[] args ) throws IOException, ParseException {
        String file = "C:\\Users\\lilpa\\IdeaProjects\\myapp\\src\\main\\java\\tester\\test.json";
        System.out.println( "Hello World!" );
        File br = new File(file);
        String str = FileUtils.readFileToString(br);
        String[] tuple = str.split("\\n");
        JSONParser parser = new JSONParser();
        JSONObject json;
        int k = tuple.length;
        for(int j = 0; j<k; j++){
            json = (JSONObject) parser.parse(tuple[j]);
            String text = (String) json.get("text");
            //System.out.println(text);
            if(text.contains("Flu")){
                System.out.println("MAGA");
            }
            if(text.contains("birds")){
                System.out.println("hereee");
            }
        }


    }
}
