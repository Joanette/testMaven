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

        int k = tuple.length;
        for(int j = 0; j<k; j++){
            json = (JSONObject) parser.parse(tuple[j]);
            String text = (String) json.get("text");
            Long id = (Long) json.get("id");
            //System.out.println(text);
            if(text.contains("MAGA")){
                System.out.println("MAGA");
                magaCollection.append(" "+id+",");
            }
            if(text.contains("birds")){
               // System.out.println("hereee");
                swampCollection.append(" "+id+",");
            }
        }
        System.out.println(swampCollection.toString());
        System.out.println(magaCollection.toString());

    }
}
