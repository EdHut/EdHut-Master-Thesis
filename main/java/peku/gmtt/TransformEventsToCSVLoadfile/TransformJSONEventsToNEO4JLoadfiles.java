package peku.gmtt.TransformEventsToCSVLoadfile;

import com.google.gson.*;
import com.google.gson.internal.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
//import java.util.TreeMap;

/**
 *
 */
public class TransformJSONEventsToNEO4JLoadfiles {

    public static void main(String[] args) {
        final Logger logger = Logger.getLogger(TransformJSONEventsToNEO4JLoadfiles.class);

        TreeMap attributes = new TreeMap();
        Integer count = 0;
        Integer lineCount = 0;
        Gson gson = new Gson();

        try (BufferedReader bufferReader = new BufferedReader(new FileReader("/Users/peterkuijpers/gmttdata/indexer_data.json"))) {
        // try (BufferedReader bufferReader = new BufferedReader(new FileReader("/Users/peterkuijpers/gmttdata/gmtt-indexer-v1.json"))) {

            try {
                // StringBuilder sb = new StringBuilder();
                String line = bufferReader.readLine();
                Entry<String, String> test;
                LinkedTreeMap<String, String> test2;

                while (line != null) {
                    lineCount++;
                    if ((lineCount % 1000) == 0) {
                        System.out.println("lineCount: " + lineCount);
                    }

                    JsonObject componentPropertiesObject = gson
                            .fromJson(line, JsonObject.class)
                            .get("event").getAsJsonObject()
                            .get("component").getAsJsonObject()
                            .get("componentProperties").getAsJsonObject();

                    Set<Map.Entry<String, JsonElement>> entries = componentPropertiesObject.entrySet();

                    for (Map.Entry<String, JsonElement> entry: entries) {
                        count = (Integer)attributes.get(entry.getKey());    // get count of occurrences for this attribute
                        if (count == null) {
                            attributes.put(entry.getKey(), 1);  // attribute not yet known, so add first entry now
                        } else {
                            attributes.put(entry.getKey(), count + 1);  // increase attribute count with 1
                        }
                    }

                    line = bufferReader.readLine();
                }
                System.out.println("File successfully processed.");

                // print all attributes that were found in the file
                System.out.println ("componentAttributes found in file:");
                for (Object key: attributes.keySet()) {
                    System.out.println(("Key: " + key + " Value: " + attributes.get(key)));
                }
                System.out.println ("------------------------");
                for (Object key: attributes.keySet()) {
                    System.out.println(key);
                }
                System.out.println ("------------------------");
                System.out.println("lineCount: " + lineCount);
            } catch (JsonSyntaxException e) {
                System.out.println("A JSON syntax exception was found.");
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
