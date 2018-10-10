package peku.gmtt.StateTransitions;

import peku.gmtt.*;
import com.google.gson.*;
import org.apache.log4j.Logger;
// import peku.gmtt.GMTTEventPropertyDefinition;
import org.apache.commons.cli.*;

import java.io.*;
import java.util.*;

public class GenerateTransitionStatistics {

    final Logger logger = Logger.getLogger(GenerateTransitionStatistics.class);

    public static void main(String[] args) {
        final Logger logger = Logger.getLogger(GenerateTransitionStatistics.class);

        HashMap<String, HashMap<String, GMTTStateTransitionStatistics>> allTransitions = new HashMap<String, HashMap<String, GMTTStateTransitionStatistics>>();
        HashMap<String, GMTTStatusHistogram> arrivalsPerStatus = new HashMap<>();
        HashMap<String, GMTTStatusHistogram> departuresPerStatus = new HashMap<>();

        // Parse the command line arguments
        Options options = new Options();

        Option inputFilename = new Option("i", "inputfile", true, "Name of csv file that contains the states per case (UUID)");
        inputFilename.setRequired(true);
        options.addOption(inputFilename);

        Option outFile = new Option("o", "outputfilename", true, "Name of file to generate");
        outFile.setRequired(true);
        options.addOption(outFile);

        Option bucketSizeArg = new Option("b", "bucketsize", true, "Size of buckets in milleseconds (default: 300000 (5 minutes)");
        bucketSizeArg.setRequired(false);
        options.addOption(bucketSizeArg);

        Option skipLine = new Option("s", "skiplines", true, "Number of lines to skip at beginning of file before starting to parse.");
        skipLine.setRequired(false);
        options.addOption(skipLine);

        Option totalLine = new Option("t", "totallines", true, "Total number of events to process");
        totalLine.setRequired(false);
        options.addOption(totalLine);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(args[0], options);

            System.exit(1);
            return;
        }

        String inputFile = cmd.getOptionValue("inputfile");
        String outputFilename = cmd.getOptionValue("outputfilename");
//        String outputType = cmd.getOptionValue("filetype");

        logger.info("Inputfile: " + inputFile);
        logger.info("Outputfile: " + outputFilename);

        int skipLines = Integer.parseInt(cmd.getOptionValue("skiplines", "0"));
        int totalLines = Integer.parseInt(cmd.getOptionValue("totallines", "100000000"));
        Long bucketSize = Long.parseLong((cmd.getOptionValue("bucketsize", "300000")));

        int lineCount = 0;

        logger.info("About to open file with state transitions.");
        // Arguments for invocation:
        // args[0]: filename of input file with status transitions (csv formatted)
        // Input file created with GenerateFilefromGMTT with option " -f trans"
        try (BufferedReader bufferReader = new BufferedReader(new FileReader(inputFile))) {
            try {
                String line = bufferReader.readLine();

                // Start with skipping a number of lines
                while (line != null & lineCount++ < skipLines) {
                    line = bufferReader.readLine();
                }

                while (line != null & totalLines-- > 0) {
                    lineCount++;
                    if ((lineCount % 10000) == 0) {
                        System.out.println("lineCount: " + lineCount);
                        System.out.println("Available memory" + getAvailableMemory());
                    }

                    String[] transition = line.split(",");

                    if (!transition[0].equals("traceID")) {
                        // If first field parsed contains value "traceID", then it's a header row.
                        // I.e. only process the rows that do not have TraceID as first value.

                        // 0  traceID
                        // 1  transtime-msec
                        // 2  from-status
                        // 3  to-status
                        // 4  statustime-epoch
                        // 5  statustime
                        // 6  reqexecdate
                        // 7  statusdayofweek
                        // 8  targetapp
                        // 9  planroutingdt
                        // 10 enrexecdate
                        // 11 bpt
                        // 12 planreleasedt
                        // 13 paymentmethod
                        // 14 cpn
                        // 15 paymentengine
                        // 16 chnl1
                        // 17 chnl2
                        // 18 msgtype

                        // line with data elements
                        GMTTStateTransition trans = new GMTTStateTransition(Long.valueOf(transition[4]) - Long.valueOf(transition[1]), Long.valueOf(transition[4]));

                        // Does this state transition already exist?
//                        if (allTransitions == null || (!allTransitions.containsKey(transition[2]))) {
                        if (!allTransitions.containsKey(transition[2])) {
                            // From-status does not exist yet, so add entry in the map
                            HashMap<String, GMTTStateTransitionStatistics> y = new HashMap<>();
                            allTransitions.put(transition[2], y);
                            GMTTStateTransitionStatistics transStats = new GMTTStateTransitionStatistics(bucketSize);
                            y.put(transition[3], transStats);
                            transStats.AddTransition(trans);
                        } else {
                            // From-status already exists. Now check does the to-status exist
                            HashMap<String, GMTTStateTransitionStatistics> x = allTransitions.get(transition[2]);
                            if (x == null || (!x.containsKey(transition[3]))) {
                                // to-status also does not exists yet
                                HashMap<String, GMTTStateTransitionStatistics> y = new HashMap<>();
                                GMTTStateTransitionStatistics transStats = new GMTTStateTransitionStatistics(bucketSize);
                                y.put(transition[3], transStats);
                                transStats.AddTransition(trans);
                            } else {
                                // statistics for the from-status and to-status exists already. Locate it.
                                GMTTStateTransitionStatistics transStats = x.get(transition[3]);
                                transStats.AddTransition(trans);
                            }
                        }

                        // Update departures histogram
                        if (departuresPerStatus.containsKey(transition[2])) {
                            departuresPerStatus.get(transition[2]).Add(Long.valueOf(transition[4]));
                        } else {
                            GMTTStatusHistogram newvalue = new GMTTStatusHistogram(bucketSize);
                            newvalue.Add(Long.valueOf(transition[4]));
                            departuresPerStatus.put(transition[2], newvalue);
                        }

                        // Update departures histogram
                        if (arrivalsPerStatus.containsKey(transition[3])) {
                            arrivalsPerStatus.get(transition[3]).Add(Long.valueOf(transition[4]));
                        } else {
                            GMTTStatusHistogram newvalue = new GMTTStatusHistogram(bucketSize);
                            newvalue.Add(Long.valueOf(transition[4]));
                            arrivalsPerStatus.put(transition[3], newvalue);
                        }

                    }
                    line = bufferReader.readLine();
                }
            } catch (JsonSyntaxException e) {
                System.out.println("A JSON syntax exception was found.");
                e.printStackTrace();
            }
            System.out.println("File successfully processed.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Calculate statistics for all state transitions


        // Write results to file
        // File 1 - details per state transition per bucket: from, to, timebucket, count, mean, stdev, min, max, count arrivals, count departures
        //for(Map.Entry<String,HashMap<String, GMTTStateTransitionStatistics>> entry : allTransitions.entrySet()) {

        //}

        try (BufferedWriter outputFile = new BufferedWriter(new FileWriter(outputFilename + "-perbucket.csv")) ) {
            outputFile.write("from-status,to-status,bucket,count,mean,stdev,min,max,arrivalcount,departurecount\n");

            allTransitions.forEach((fromStatus,stats)-> {  // iterate HashMap Java 8 style with lambda
                stats.forEach((toStatus, value)-> {
                    // For each from-state and to-state, iterate the histogram
                    try {
                        Map<Long, Integer> histogramMap = value.getHistogramMap();

                        for (Long i = value.getFirstBucket(); i < value.getLastBucket(); i++) {

                            outputFile.write(fromStatus +","+ toStatus +","+ i);

                            if (histogramMap.containsKey(i)) {

                                outputFile.write("\n");
                            } else {
                                outputFile.write(",0,,,,,,\n");
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        // File 2 - details per state transition: from, to, count, mean, stdev, min, max





        logger.info("Program execution completed.");
    }

    public static long getAvailableMemory() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory(); // current heap allocated to the VM process
        long freeMemory = runtime.freeMemory(); // out of the current heap, how much is free
        long maxMemory = runtime.maxMemory(); // Max heap VM can use e.g. Xmx setting
        long usedMemory = totalMemory - freeMemory; // how much of the current heap the VM is using
        long availableMemory = maxMemory - usedMemory; // available memory i.e. Maximum heap size minus the current amount used
        return availableMemory;
    }

}
