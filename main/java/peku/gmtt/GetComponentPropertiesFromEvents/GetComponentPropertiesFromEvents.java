package peku.gmtt.GetComponentPropertiesFromEvents;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class GetComponentPropertiesFromEvents {

    public static void main(String[] args) {
        final Logger logger = Logger.getLogger(GetComponentPropertiesFromEvents.class);

        TreeMap componentProperties = new TreeMap();
        TreeMap eventProperties = new TreeMap();
        TreeMap componentAttributes = new TreeMap();
        Integer count;
        int lineCount = 0;
        Gson gson = new Gson();

        logger.info("About to open file with events.");
        // Arguments for invocation:
        // args[0]: filename of input file with events
        // args[1]: filename of output file (csv formatted)
        try (BufferedReader bufferReader = new BufferedReader(new FileReader(args[0]))) {
            try {
                String line = bufferReader.readLine();
                // Entry<String, String> test;
                // LinkedTreeMap<String, String> test2;

                while (line != null) {
                    lineCount++;
                    if ((lineCount % 1000) == 0) {
                        System.out.println("lineCount: " + lineCount);
                    }

                    JsonObject event = gson
                            .fromJson(line, JsonObject.class)
                            .get("event").getAsJsonObject()
                            ;

                    Set<Map.Entry<String, JsonElement>> eventEntries = event.entrySet();
                    for (Map.Entry<String, JsonElement> entry: eventEntries) {
                        if (entry.getValue().isJsonPrimitive()) {
                            count = (Integer)eventProperties.get(entry.getKey());    // get count of occurrences for this attribute
                            if (count == null) {
                                eventProperties.put(entry.getKey(), 1);  // attribute not yet known, so add first entry now
                            } else {
                                eventProperties.put(entry.getKey(), count + 1);  // increase attribute count with 1
                            }
                        }
                    }

                    JsonObject component = event
                            .get("component").getAsJsonObject()
                            ;

                    Set<Map.Entry<String, JsonElement>> componentEntries = component.entrySet();

                    for (Map.Entry<String, JsonElement> entry: componentEntries) {
                        if (entry.getValue().isJsonPrimitive()) {
                            count = (Integer) componentAttributes.get(entry.getKey());    // get count of occurrences for this attribute
                            if (count == null) {
                                componentAttributes.put(entry.getKey(), 1);  // attribute not yet known, so add first entry now
                            } else {
                                componentAttributes.put(entry.getKey(), count + 1);  // increase attribute count with 1
                            }
                        }
                    }

                    JsonObject componentPropertiesObject = component
                            .get("componentProperties").getAsJsonObject()
                            ;

                    Set<Map.Entry<String, JsonElement>> entries = componentPropertiesObject.entrySet();

                    for (Map.Entry<String, JsonElement> entry: entries) {
                        if (entry.getValue().isJsonPrimitive()) {
                            count = (Integer) componentProperties.get(entry.getKey());    // get count of occurrences for this attribute
                            if (count == null) {
                                componentProperties.put(entry.getKey(), 1);  // attribute not yet known, so add first entry now
                            } else {
                                componentProperties.put(entry.getKey(), count + 1);  // increase attribute count with 1
                            }
                        }
                    }

                    line = bufferReader.readLine();
                }
                System.out.println("File successfully processed.");

                // print all attributes that were found in the file
                System.out.println ("eventProperties found in file:");
                for (Object key: eventProperties.keySet()) {
                    System.out.println(("Key: " + key + " Value: " + eventProperties.get(key)));
                }
                System.out.println ("------------------------");
                System.out.println ("componentAttributes found in file:");
                for (Object key: componentAttributes.keySet()) {
                    System.out.println(("Key: " + key + " Value: " + componentAttributes.get(key)));
                }
                System.out.println ("------------------------");
                System.out.println ("componentProperties found in file:");
                for (Object key: componentProperties.keySet()) {
                    System.out.println(("Key: " + key + " Value: " + componentProperties.get(key)));
                }
                System.out.println ("------------------------");
                for (Object key: componentProperties.keySet()) {
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

        // Second stage: writing results to a file
        // Approach is to re-read the entire file, parse the events and then iterate through all earlier found attributes
        try (BufferedWriter bw            = new BufferedWriter(new FileWriter(args[1] + "_all.csv"));
             BufferedWriter bw_components = new BufferedWriter(new FileWriter(args[1] + "_components.csv"));
             BufferedWriter bw_events     = new BufferedWriter(new FileWriter(args[1] + "_events.csv"));
             BufferedWriter bw_rel_eventcomponent = new BufferedWriter(new FileWriter(args[1] + "_rel_eventcomponent.csv"));
             BufferedReader bufferReader  = new BufferedReader(new FileReader(args[0])) ) {

            TreeMap componentIds = new TreeMap();   // TreeMap to use for duplicate check on componentIds

            // Start with writing the first line which lists all attribute names
            Boolean firstKey = true;
            for (Object key: eventProperties.keySet()) {
                if (firstKey) {
                    firstKey = false;
                } else {
                    bw.write(",");
                }
                bw.write((String)key);
            }
            for (Object key: componentAttributes.keySet()) {
                bw.write("," + key);
            }
/*
            for (Object key: componentProperties.keySet()) {
                bw.write("," + key);
            }
*/
            bw.write("\n"); // Close the header line

            //bw_components.write("componentId:ID(Component)\n");
            //bw_events.write("eventId:ID(Event),eventDateTime:LONG,componentStatusDateTime:LONG\n");
            // bw_rel_eventcomponent.write ("eventId:ID(Event),componentId:ID(Component)\n");
            // bw_rel_eventcomponent.write (":START_ID(Event),:END_ID(Component)\n");
            //bw_rel_eventcomponent.write (":START_ID,:END_ID,:TYPE\n");

            // Writing content to CSV file. Each consists of 3 parts: event attributes, component attributes and finally componentProperties attributes
            // In case an attribute has no value, only a comma is written (i.e. the value is not present)
            try {
                String line = bufferReader.readLine();

                while (line != null) {
                    lineCount--;
                    if ((lineCount % 1000) == 0) {
                        System.out.println("lineCount: " + lineCount);
                    }

                    // ----------------------[ Event ]---------
                    JsonObject event = gson
                            .fromJson(line, JsonObject.class)
                            .get("event").getAsJsonObject()
                            ;

                    firstKey = true;
                    for (Object key: eventProperties.keySet()) {
                        if (firstKey) {
                            firstKey = false;
                        } else {
                            bw.write(",");
                        }
                        // JsonPrimitive x = event.get(key.toString()).getAsJsonPrimitive();
                        String tmp = event.get(key.toString()).getAsString();
                        bw.write(event.get(key.toString()).getAsString());
                    }

                    // ----------------------[ Component ]---------
                    JsonObject component = event
                            .get("component").getAsJsonObject()
                            ;

                    for (Object key: componentAttributes.keySet()) {
                        bw.write(",");
                        if (component.get(key.toString()) != null) {
                            bw.write(component.get(key.toString()).getAsString());
                        }
                    }
                    // -----[only for components.csv]
                    if (component.get("componentID") != null) {
                        if (componentIds.get(component.get("componentID").getAsString()) == null) {
                            bw_components.write(component.get("componentID").getAsString() + "\n");
                            componentIds.put(component.get("componentID").getAsString(), 1);
                        }
                    }

                    // -----[only for events.csv]
                    if (event.get("enterpriseMessageID") != null) {
                        bw_events.write(event.get("enterpriseMessageID").getAsString() + ",");
                        bw_events.write(event.get("eventDateTime").getAsString() + ",");
                        bw_events.write(component.get("componentStatusDateChangeTime").getAsString() + "\n");
                    }

                    // -----[only for rel_eventcomponent.csv]
                    if (event.get("enterpriseMessageID") != null) {
                        if (component.get("componentID") != null) {
                            if (!"".equals(component.get("componentID").getAsString())) {
                                bw_rel_eventcomponent.write(event.get("enterpriseMessageID").getAsString() + "," + component.get("componentID").getAsString() + ",CONCERNS\n");
                            }
                        }
                    }

                    // ----------------------[ ComponentProperties ]--------
                    JsonObject componentPropertiesObject = component
                            .get("componentProperties").getAsJsonObject()
                            ;
/*
                    for (Object key: componentProperties.keySet()) {
                        bw.write(",");
                        if (componentPropertiesObject.get(key.toString())!= null) {
                            bw.write(componentPropertiesObject.get(key.toString()).getAsString());
                        }
                    }
*/
                    bw.write("\n");
                    line = bufferReader.readLine();
                }
                System.out.println("File successfully processed.");

            } catch (JsonSyntaxException e) {
                System.out.println("A JSON syntax exception was found.");
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Program execution completed.");
    }
}
