package peku.gmtt.FindCases;

import peku.gmtt.*;
import com.google.gson.*;
import org.apache.log4j.Logger;
import peku.gmtt.GMTTEventPropertyDefinition;
import org.apache.commons.cli.*;

import java.io.*;
import java.lang.ref.WeakReference;
import java.text.SimpleDateFormat;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.time.*;

public class GenerateFilefromGMTT {

    public static void main(String[] args) {
        final Logger logger = Logger.getLogger(GenerateFilefromGMTT.class);

        HashMap<String, Integer> blacklistedApplication = new HashMap<>();
        blacklistedApplication.put("RTPE", 0);
        blacklistedApplication.put("IRM", 0);

        HashMap<String, String> obfuscateApplication = new HashMap<>();

        obfuscateApplication.put("GIOM-001", "GIOM");
        obfuscateApplication.put("Payment-Legacy-Adapter", "PLA");
        obfuscateApplication.put("GCB_TFBG_GIOM", "TFBG");
        obfuscateApplication.put("FEX_[P]", "FEX");
        obfuscateApplication.put("Profile", "PROF");
        obfuscateApplication.put("CBP_PIE", "Portal");
        obfuscateApplication.put("Calypso", "FMBT");
        obfuscateApplication.put("TIplus", "TIPL");
        obfuscateApplication.put("LoanIQ", "LOAN");
        obfuscateApplication.put("IRM", "IRM");
        obfuscateApplication.put("INVE", "INVE");
        obfuscateApplication.put("GCB_GIFM_GIOM_PaymentOrder", "IRMT");
        obfuscateApplication.put("GPEE_AuthenticationBridge", "DVT");
        obfuscateApplication.put("Dovetail", "DVT");
        obfuscateApplication.put("GCB_GPEE_PaymentOrder", "GPEE_PO");
        obfuscateApplication.put("ing-lu-giom", "LUOM");
        obfuscateApplication.put("GCB_GCME_PaymentOrder", "GCME_PO");
        obfuscateApplication.put("GCB_GPEE_PaymentOrder_GIFM", "GPEE_PO_GIFM");
        obfuscateApplication.put("RTB-Profile-Adapter", "RTBP7");
        obfuscateApplication.put("FV", "FV");
        obfuscateApplication.put("RTPE", "RTPE");
        obfuscateApplication.put("IBP", "IBP");
        obfuscateApplication.put("SPK", "SPK");
        obfuscateApplication.put("SPE", "SPE");
        obfuscateApplication.put("GCB_GINT_GIOM", "IBPGIOM");
        obfuscateApplication.put("BOR-Routing-Component", "BOR_RC");
        obfuscateApplication.put("DSS-Dovetail Screening Service", "DVTSCR");

        Map<String, GMTTEventPropertyDefinition> includeProperties = new HashMap<>();

        includeProperties.put("PmtInf-ReqdExctnDt", new GMTTEventPropertyDefinition("time:reqexecdate", "date", "reqexecdate"));
        includeProperties.put("ENRCHD_EXE_DT", new GMTTEventPropertyDefinition("time:enrexecdate", "date", "enrexecdate"));                // GIOM
        includeProperties.put("COMMERCIALPRODUCTNAME", new GMTTEventPropertyDefinition("concept:cpn", "string", "cpn"));
        includeProperties.put("INGOrgnlChanl", new GMTTEventPropertyDefinition("concept:chnl1", "string", "chnl1"));
        includeProperties.put("ORGNL_CHNNL", new GMTTEventPropertyDefinition("concept:chnl2", "string", "chnl2"));                    // GIOM
        includeProperties.put("MessageType", new GMTTEventPropertyDefinition("concept:msgtype", "string", "msgtype"));
        includeProperties.put("BPT", new GMTTEventPropertyDefinition("concept:bpt", "string", "bpt"));                             // RTPE
        includeProperties.put("PAYMENT_METHOD", new GMTTEventPropertyDefinition("concept:paymentmethod", "string", "paymentmethod"));        // GIOM
        includeProperties.put("PMT_INSTR_PE_NM", new GMTTEventPropertyDefinition("concept:paymentengine", "string", "paymentengine"));       // GIOM
        includeProperties.put("TargetApp", new GMTTEventPropertyDefinition("concept:targetapp", "string", "targetapp"));                 // PLA
        includeProperties.put("PLANNED_DATE_TIME_ROUTING", new GMTTEventPropertyDefinition("concept:planroutingdt", "date", "planroutingdt"));      // GIOM?
        includeProperties.put("PLANNED_DT_REL_PAYMENTENGINE", new GMTTEventPropertyDefinition("concept:planreleasedt", "date", "planreleasedt"));   // GIOM?
        // Plus some properties expressed in another way
        // StatusDayOfWeek: which day number within the week did the event happen
        includeProperties.put("statusDOW", new GMTTEventPropertyDefinition("concept:statusDayOfWeek", "string", "statusdayofweek"));

        // Parse the command line arguments
        Options options = new Options();

        Option inputFilename = new Option("i", "inputfile", true, "Name of file that contains the GMTT events");
        inputFilename.setRequired(true);
        options.addOption(inputFilename);

        Option outFile = new Option("o", "outputfilename", true, "Name of file to generate");
        outFile.setRequired(true);
        options.addOption(outFile);

        Option fileType = new Option("f", "filetype", true, "Format of output. Values: csv, xes, trans");
        fileType.setRequired(true);
        options.addOption(fileType);

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
        String outputType = cmd.getOptionValue("filetype");

        logger.info("Inputfile: " + inputFile);
        logger.info("Outputfile: " + outputFilename + "." + outputType);

        int skipLines = Integer.parseInt(cmd.getOptionValue("skiplines", "0"));
        int totalLines = Integer.parseInt(cmd.getOptionValue("totallines", "100000000"));

        int lineCount = 0;
        Gson gson = new Gson();

        // Map will all components, identified by their ComponentId (uuid)
        Map<UUID, GMTTComponent> Components = new HashMap<>();

        logger.info("About to open file with events.");
        // Arguments for invocation:
        // args[0]: filename of input file with events (json formatted)
        try (BufferedReader bufferReader = new BufferedReader(new FileReader(inputFile))) {
            try {
                boolean memoryOkay = true;
                String line = bufferReader.readLine();

                // Start with skipping a number of lines
                while (line != null & lineCount++ < skipLines) {
                    line = bufferReader.readLine();
                }

                while (line != null & totalLines-- > 0 & memoryOkay) {
                    lineCount++;
                    if ((lineCount % 10000) == 0) {
                        System.out.println("lineCount: " + lineCount);
                        long availableMemory = getAvailableMemory();
                        System.out.println("Available memory: " + availableMemory);

                        // Ensure sufficient memory available. If needed, force a GC
                        memoryOkay = (availableMemory > 1500000000);
                        if (!memoryOkay) {
                            System.out.println("Forcing GC");
                            Object object = new Object();
                            final WeakReference<Object> ref = new WeakReference<>(object);
                            object = null;
                            while (ref.get() != null) {
                                System.gc();
                            }
                            availableMemory = getAvailableMemory();
                            System.out.println("Available memory: " + availableMemory);
                            memoryOkay = (availableMemory > 1500000000);
                        }
                    }

                    // Parse the JSON into event, component and componentproperties.
                    JsonObject event = gson
                            .fromJson(line, JsonObject.class)
                            .get("event").getAsJsonObject()
                            ;

                    if (event.has("component")) {
                        JsonObject component = event
                                .get("component").getAsJsonObject();
                        JsonObject componentPropertiesObject = component
                                .get("componentProperties").getAsJsonObject();
                        String appName = component.get("applicationName").getAsString();

                        if (blacklistedApplication.containsKey(appName)) {
                            // Don't process events of applications that we're not interested in. Count ho many events were ignored.
                            Integer i = blacklistedApplication.get(appName);
                            blacklistedApplication.put(appName, ++i);
                        } else {
                            // Handle some special cases to make status unique across whole set
                            String prefixStatus = "";
                            // GIOM File/Msg, Order and Trx levels
                            if ("GIOM-001".equals(component.get("applicationName").getAsString())) {
                                boolean bFileId = componentPropertiesObject.has("PFL_ID");
                                boolean bMsgId = componentPropertiesObject.has("PMI_ID");
                                boolean bInstrId = componentPropertiesObject.has("PII_ID");

                                if (bFileId & bMsgId & bInstrId) {
                                    prefixStatus = "trx-";
                                } else if (bFileId & bMsgId) {
                                    prefixStatus = "order-";
                                } else if (componentPropertiesObject.has("CUST_MSG_ID")) {
                                    prefixStatus = "msg-";
                                } else if (componentPropertiesObject.has("CUST_FL_NM")) {
                                    prefixStatus = "file-";
                                }
                            }

                            String applicationName = (component.has("applicationName") ? component.get("applicationName").getAsString() : null);
                            String obfuscatedApplicationName = null;
                            if (applicationName != null) {
                                obfuscatedApplicationName = obfuscateApplication.get(applicationName);
                                if (obfuscatedApplicationName == null) {
                                    logger.info("No obfuscation for:" + applicationName);
                                    obfuscateApplication.put(applicationName, applicationName);
                                    obfuscatedApplicationName = applicationName;
                                }
                            }

                            GMTTEvent eventDetails = new GMTTEvent(
                                    (component.has("componentStatus") ? prefixStatus + component.get("componentStatus").getAsString() : null)
                                    , (component.has("componentStatusDateChangeTime") ? component.get("componentStatusDateChangeTime").getAsLong() : null)
                                    , obfuscatedApplicationName
//                                    , (component.has("buildingBlockName") ? component.get("buildingBlockName").getAsString() : null)
//                                    , (component.has("hostName") ? component.get("hostName").getAsString() : null)
                            );

                            // Check presence of certain whitelisted componentproperties. If present, add them as an eventdetail
                            for (Map.Entry<String, GMTTEventPropertyDefinition> entry : includeProperties.entrySet()) {
                                if (componentPropertiesObject.has(entry.getKey())) {
                                    eventDetails.addProperty(entry.getValue().getPropertyName(), new GMTTEventProperty(
                                            componentPropertiesObject.get(entry.getKey()).getAsString(),
                                            entry.getValue()
                                    ));
                                    // TODO: add extra derived properties: planroutingdt and reqexecdate expressed as Long timestamp of start of day or as delta between statusdatetime and reqexecdate
                                /*
                                if ("PLANNED_DT_REL_PAYMENTENGINE".equals(entry.getKey())) {

                                } else if ("ENRCHD_EXE_DT".equals(entry.getKey())) {

                                }
                                */

                                }
                            }

                            // Add extra derived properties: dayofweek (of status datetime)
                            Instant instant = Instant.ofEpochMilli(component.get("componentStatusDateChangeTime").getAsLong());
                            ZonedDateTime zdt = instant.atZone(ZoneId.of("Europe/Amsterdam"));
                            DayOfWeek statusDayOfWeek = zdt.getDayOfWeek();
//                        eventDetails.addProperty("statusDOW", new GMTTEventProperty(String.valueOf(statusDayOfWeek.getValue()), "string", "concept:statusDayOfWeek"));
                            eventDetails.addProperty("statusDOW", new GMTTEventProperty(String.valueOf(statusDayOfWeek.getValue()), includeProperties.get("statusDOW")));

                            // Is ComponentId not null and is it not seen earlier?
                            //logger.info(component.get("componentID").getAsString());
                            UUID componentId = null;
                            UUID parentComponentId = null;
                            if (component.has("componentID")) {
                                componentId = UUID.fromString(component.get("componentID").getAsString());
                            }
                            if (component.has("parentComponentID")) {
                                parentComponentId = UUID.fromString(component.get("parentComponentID").getAsString());
                            }

                            if (componentId != null) {
                                // Try to find the componentId in the known set of components.
                                GMTTComponent newComponent = Components.get(componentId);

                                if (newComponent == null) {
                                    // Component was not seen earlier, so create it and add it to Components set
                                    newComponent = new GMTTComponent(componentId, parentComponentId);
                                    Components.put(componentId, newComponent);
                                    if (parentComponentId != null) {
                                        GMTTComponent parentComponent = Components.get(parentComponentId);

                                        if (parentComponent == null) {
                                            newComponent.setHighestComponent(false);    // it has a parent, so is not highest in hierarchy
                                            parentComponent = new GMTTComponent(parentComponentId);
                                            Components.put(parentComponentId, parentComponent);
                                        } else {
                                            if (parentComponent.isSuitableForParent()) {
                                                newComponent.setHighestComponent(false);
                                            }
                                        }

                                        // Add reference from component to its parent (needed from traversing 'up' the hierarchy
                                        newComponent.setParentComponent(parentComponent);
                                        // And at last, add the new component to its parent (it may become one of many child components)
                                        parentComponent.addComponentToParentComponent(newComponent, eventDetails.getComponentStatusDateTime());
                                    }
                                } else {
                                    // Component is already known. But the new event may contain new info, so enrich component with those details
                                    if (newComponent.getParentComponentID() == null) {
                                        // it did not have a parentcomponent yet....
                                        if (parentComponentId != null) {
                                            // but new event contains reference to parent
                                            newComponent.setParentComponentID(parentComponentId);

                                            GMTTComponent parentComponent = Components.get(parentComponentId);
                                            if (parentComponent == null) {
                                                // although the parentComponentID is in the event, the parentcomponent itself was not seen yet, so create it now
                                                parentComponent = new GMTTComponent(parentComponentId);
                                                Components.put(parentComponentId, parentComponent);
                                            } else {
                                                // If a new reference to a parent is found, then only of that parent is suitable to act as parent, mark the new component as not being the highest in the hierarchy
                                                if (parentComponent.isSuitableForParent()) {
                                                    newComponent.setHighestComponent(false);
                                                }
                                            }
                                            // And at last, add the new component to its parent
                                            parentComponent.addComponentToParentComponent(newComponent, eventDetails.getComponentStatusDateTime());
                                        }
                                    } else {
                                        if (parentComponentId != null) {
                                            // both the already earlier registered component had a parent and the event that is now being processed: check of it's consistent (i.e. same value for parent)
                                            if (!newComponent.getParentComponentID().equals(parentComponentId)) {
                                                // logger.info("Inconsistency at line:" + lineCount + "  Parents differ for component:" + componentId + "Value 1:" + parentComponentId + " Value 2:" + newComponent.getParentComponentID());
                                            }
                                        }
                                    }
                                }
                                newComponent.AddEventToComponent(eventDetails);
                            }
                        }
                    } else {
                        if (!event.has("dirtyDataMap")) {
                            logger.info("Event found without component entry in json.");
                        }

                    }

                    line = bufferReader.readLine();
                }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (JsonSyntaxException e) {
                    System.out.println("A JSON syntax exception was found.");
                    e.printStackTrace();
                }
                System.out.println("File successfully processed.");
                System.out.println("Processed till Line number: " + lineCount);

                blacklistedApplication.forEach((k,v)->System.out.println("Application: " + k + " Ignored events: " + v));

            } catch (IOException e) {
                e.printStackTrace();
            }

        switch (outputType) {
            case "xes":
                GenerateXESfile(Components, outputFilename);
                break;
            case "csv":
                GenerateCSVfile(Components, includeProperties, outputFilename);
                break;
            case "trans":
                GenerateStateTransitionsfile(Components, includeProperties, outputFilename);
                break;
        }

        logger.info("Program execution completed.");
    }

    private static void GenerateXESfile(Map<UUID, GMTTComponent> Components, String outputFilename) {
        final Logger logger = Logger.getLogger(GenerateFilefromGMTT.class);

        try (BufferedWriter outputFile = new BufferedWriter(new FileWriter(outputFilename + ".xes")) ) {
            outputFile.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n");
            outputFile.write("\t<log xes.version=\"2.0\" xes.features=\"nested-attributes\" xmlns=\"http://www.xes-standard.org\">\n");

            outputFile.write("\t<extension name=\"Time\" prefix=\"time\" uri=\"http://www.xes-standard.org/time.xesext\"/>\n");
            outputFile.write("\t<extension name=\"Concept\" prefix=\"concept\" uri=\"http://www.xes-standard.org/concept.xesext\" />\n");
            outputFile.write("\t<extension name=\"Organizational\" prefix=\"org\" uri=\"http://www.xes-standard.org/org.xesext\"/>\n");
            outputFile.write("\t<extension name=\"Lifecycle\" prefix=\"lifecycle\" uri=\"http://www.xes-standard.org/lifecycle.xesext\"/>\n");

            outputFile.write(
                    "\t<global scope=\"trace\">\n" +
                            "\t\t<string key=\"concept:name\" value=\"UNKNOWN\"/>\n" +
                            "\t</global>\n");
            outputFile.write(
                    "\t<global scope=\"event\">\n" +
                            "\t\t<date key=\"time:timestamp\" value=\"1970-01-01T00:00:00.000+00:00\" />\n" +
                            "\t\t<string key=\"org:application\" value=\"UNKNOWN\" />\n" +
                            "\t\t<string key=\"concept:name\" value=\"UNKNOWN\" />\n" +
                            "\t\t<string key=\"lifecycle:transition\" value=\"complete\"/>\n" +
                            "\t</global>\n");

            int maxEvents = 0;
            int numTraces = 0;
            for (GMTTComponent component : Components.values()) {
                if (component.isHighestComponent()) {
                    int i = component.getNumberOfChildren();
                    if (i > 2) {    // only write to file if at least a number of events in a single trace
                        if (i > maxEvents) {
                            maxEvents = i;
                        }
                        numTraces++;
                        if ((numTraces % 10000) == 0) {
                            logger.info("Intermediate count number of traces: " + numTraces + " traces.");
                            outputFile.flush();
                        }

                        outputFile.write("\t<trace>\n");
                        outputFile.write("\t\t<string key=\"concept:name\" value=\"" + component.getComponentID() + "\"/>\n");
                        component.writeXESoutput(outputFile);
                        outputFile.write("\t</trace>\n");
                    }
                }

            }

            logger.info("Number of traces: " + numTraces + " traces.");
            logger.info("Longest trace: " + maxEvents + " events.");

            outputFile.write("</log>\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void GenerateCSVfile(Map<UUID, GMTTComponent> Components, Map<String, GMTTEventPropertyDefinition> includeProperties, String outputFilename) {
        final Logger logger = Logger.getLogger(GenerateFilefromGMTT.class);

        try (BufferedWriter outputFile = new BufferedWriter(new FileWriter(outputFilename + ".csv")) ) {

            // Header row of the csv file
//            outputFile.write("traceID, status, buildingblock, statustime-epoch, statustime");
            outputFile.write("traceID, status, statustime-epoch, statustime");
            for (GMTTEventPropertyDefinition prop : includeProperties.values()) {
                outputFile.write("," + prop.getPropertyNameCSV());
            }
            outputFile.write("\n");

            int maxEvents = 0;
            int numTraces = 0;

            for (GMTTComponent component : Components.values()) {
                if (component.isHighestComponent()) {
                    int i = component.getNumberOfChildren();
                    if (i > maxEvents) {
                        maxEvents = i;
                    }
                    numTraces++;

                    UUID traceID = component.getComponentID();    // this is the identifier of the trace. In CSV this is first column of each row

                    component.writeCSVoutput(outputFile, traceID, includeProperties);
                }
                if ((numTraces % 10000) == 0) {
                    logger.info("Intermediate count number of traces: " + numTraces + " traces.");
                    outputFile.flush();
                }
            }

            logger.info("Number of traces: " + numTraces + "traces.");
            logger.info("Longest trace: " + maxEvents + "events.");
            WriteTopParent(Components);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void GenerateStateTransitionsfile(Map<UUID, GMTTComponent> Components, Map<String, GMTTEventPropertyDefinition> includeProperties, String outputFilename) {
        final Logger logger = Logger.getLogger(GenerateFilefromGMTT.class);

        try (BufferedWriter outputFile = new BufferedWriter(new FileWriter(outputFilename + "-trans.csv")) ) {

            // Header row of the csv file
            outputFile.write("traceID,transtime-msec,from-status,to-status,statustime-epoch,statustime");
            for (GMTTEventPropertyDefinition prop : includeProperties.values()) {
                outputFile.write("," + prop.getPropertyNameCSV());
            }
            outputFile.write("\n");

            int maxEvents = 0;
            int numTraces = 0;

            for (GMTTComponent component : Components.values()) {
                if (component.isHighestComponent()) {
                    int i = component.getNumberOfChildren();
                    if (i > maxEvents) {
                        maxEvents = i;
                    }
                    numTraces++;

                    UUID traceID = component.getComponentID();    // this is the identifier of the trace.

                    component.writeStateTransitions(outputFile, traceID, includeProperties);
                }
                if ((numTraces % 10000) == 0) {
                    logger.info("Intermediate count number of traces: " + numTraces + " traces.");
                    outputFile.flush();
                }
            }

            logger.info("Number of traces: " + numTraces + "traces.");
            logger.info("Longest trace: " + maxEvents + "events.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void WriteTopParent(Map<UUID, GMTTComponent> Components) {
        final Logger logger = Logger.getLogger(GenerateFilefromGMTT.class);

        GMTTComponent mostDirectChildren = null;

        for (GMTTComponent component : Components.values()) {
            if ((mostDirectChildren == null)) {
                mostDirectChildren = component;
            } else if (component.getNumberOfDirectChildren() > mostDirectChildren.getNumberOfDirectChildren()) {
                mostDirectChildren = component;
            }
        }

        logger.info("Component with most direct children:");
        logger.info("componentId: " + mostDirectChildren.getComponentID());
        logger.info("Number of children:" + mostDirectChildren.getNumberOfDirectChildren());
    }

    private static long getAvailableMemory() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory(); // current heap allocated to the VM process
        long freeMemory = runtime.freeMemory(); // out of the current heap, how much is free
        long maxMemory = runtime.maxMemory(); // Max heap VM can use e.g. Xmx setting
        long usedMemory = totalMemory - freeMemory; // how much of the current heap the VM is using
        return maxMemory - usedMemory;
    }

}
