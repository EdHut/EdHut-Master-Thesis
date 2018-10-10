package peku.gmtt;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GMTTEvent {

    private String componentStatus;
    private Long componentStatusDateTime;
    private String applicationName;
//    private String buildingBlockName;
//    private String hostName;

    private Map<String, GMTTEventProperty> properties = new HashMap<>();

//    public GMTTEvent(String componentStatus, Long componentStatusDateTime, String applicationName, String buildingBlockName, String hostName) {
    public GMTTEvent(String componentStatus, Long componentStatusDateTime, String applicationName) {
        this.componentStatus = componentStatus;
        this.componentStatusDateTime = componentStatusDateTime;
        this.applicationName = applicationName;
//        this.buildingBlockName = buildingBlockName;
//        this.hostName = hostName;
    }

    public String getComponentStatus() {
        return componentStatus;
    }

    public Long getComponentStatusDateTime() {
        return componentStatusDateTime;
    }

    public String getApplicationName() {
        return applicationName;
    }

//    public String getBuildingBlockName() {
//        return buildingBlockName;
//    }

//    public String getHostName() {
//        return hostName;
//    }

    public void addProperty (String key, GMTTEventProperty value) {
        properties.put(key, value);
    }
    public Map<String, GMTTEventProperty> getProperties() { return properties; }

    public void writeXESoutput (BufferedWriter outputFile) throws IOException {

        SimpleDateFormat dateformat = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

        outputFile.write("\t\t<event>\n");
        outputFile.write("\t\t\t<string key=\"concept:name\" value=\"" + applicationName + "-" + componentStatus + "\"/>\n");
//        outputFile.write("\t\t\t<string key=\"org:bb\" value=\"" + buildingBlockName +"\"/>\n");
        outputFile.write("\t\t\t<date key=\"time:timestamp\" value=\""+ dateformat.format(componentStatusDateTime) + "\"/>\n");
        //outputFile.write("\t\t\t<string key=\"lifecycle:transition\" value=\"complete\"/>\n");        // Note: some ProM plugins require explicit lifecycle events

        // write any properties as well
        for (GMTTEventProperty prop : properties.values()) {
            outputFile.write("\t\t\t<" + prop.getXESTag() + " key=\"" + prop.getXESKey() + "\" value=\"" + prop.getPropertyValue() + "\"/>\n");
        }
        outputFile.write("\t\t</event>\n");
    }

    public void writeCSVoutput (BufferedWriter outputFile, UUID traceID, Map<String, GMTTEventPropertyDefinition> includeProperties) throws IOException {

        SimpleDateFormat dateformat = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

        outputFile.write(traceID + ",");
        outputFile.write(applicationName + "-" + componentStatus + ",");
//        outputFile.write(buildingBlockName + ",");
        outputFile.write(componentStatusDateTime + ",");
        outputFile.write(dateformat.format(componentStatusDateTime));
        //outputFile.write("\t\t\t<string key=\"lifecycle:transition\" value=\"complete\"/>\n");

        // write any properties as well
        // Approach is different when compared to XES, because CSV requires fixed positions for values, with empty value in case a property is not present in the event
        for (Map.Entry<String, GMTTEventPropertyDefinition> prop: includeProperties.entrySet()) {
            outputFile.write(",");
            if (properties.containsKey(prop.getValue().getPropertyName())) {
                GMTTEventProperty i = properties.get(prop.getValue().getPropertyName());
                outputFile.write(i.getPropertyValue());
            }
        }

        outputFile.write("\n");
    }

    public void writeStateTransitionOutput (BufferedWriter outputFile, UUID traceID, Map<String, GMTTEventPropertyDefinition> includeProperties, GMTTEvent GMTTEventPrevious) throws IOException {

        SimpleDateFormat dateformat = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

        outputFile.write(traceID + ",");
        // Write the time of state transition from previous status to the current one, plus the previous status
        if (GMTTEventPrevious == null) {
            // This first event of a new case. Write the 'new' status as the previous value, plus the status of current event.
            outputFile.write("0,new,");
            outputFile.write(applicationName + "-" + componentStatus + ",");
//            outputFile.write(buildingBlockName + ",");
            outputFile.write(componentStatusDateTime + ",");
            outputFile.write(dateformat.format(componentStatusDateTime));
            // Write the properties, but since this is the initial status, no actual property values are know yet (i.e. just ensure all relevant commas are written)
            for (Map.Entry<String, GMTTEventPropertyDefinition> prop: includeProperties.entrySet()) {
                outputFile.write(",");
            }
        } else {
            // @TODO: consider als adding more 'previous' events (-1, -2, -3) (n-gram), because event sequences may(!) be predictor for a next event
            outputFile.write( (componentStatusDateTime - GMTTEventPrevious.getComponentStatusDateTime())+ "," + GMTTEventPrevious.getApplicationName() + "-" + GMTTEventPrevious.getComponentStatus() + ",");
            outputFile.write(applicationName + "-" + componentStatus + ",");
//            outputFile.write(buildingBlockName + ",");
            outputFile.write(componentStatusDateTime + ",");
            outputFile.write(dateformat.format(componentStatusDateTime));

            // Write any properties as well
            // Note 1: Approach is different when compared to XES, because CSV requires fixed positions for values, with empty value in case a property is not present in the event
            // Note 2: in csv for state transitions, the properties needed for a learning algorithm are those of the previous event, because those are the properties
            //         available when trying to predict the event the is in this loop the event that already occurred.
            for (Map.Entry<String, GMTTEventPropertyDefinition> prop: includeProperties.entrySet()) {
                outputFile.write(",");
                if (GMTTEventPrevious.getProperties().containsKey(prop.getValue().getPropertyName())) {
                    GMTTEventProperty i = GMTTEventPrevious.getProperties().get(prop.getValue().getPropertyName());
                    outputFile.write(i.getPropertyValue());
                }
            }

            // Any property of the previous event that does not have a value in the current event, will be carried to the current event
            // Reason: when learning an algorithm, a property that was received in an 'early' event in the trace may turn out to be a good predictor
            GMTTEventPrevious.getProperties().forEach(properties::putIfAbsent);
        }
        outputFile.write("\n");
    }

    void writeStateTransitionOutputAsFinalstatus(BufferedWriter outputFile, UUID traceID, Map<String, GMTTEventPropertyDefinition> includeProperties) throws IOException {
        // Will be invoked if an event is the last one for a case.
        SimpleDateFormat dateformat = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

        outputFile.write(traceID + ",");
        outputFile.write("0," + applicationName + "-" + componentStatus + ",final,");
//        outputFile.write(buildingBlockName + ",");
        outputFile.write(componentStatusDateTime + ",");
        outputFile.write(dateformat.format(componentStatusDateTime));

        for (Map.Entry<String, GMTTEventPropertyDefinition> prop: includeProperties.entrySet()) {
            outputFile.write(",");
            if (properties.containsKey(prop.getValue().getPropertyName())) {
                GMTTEventProperty i = properties.get(prop.getValue().getPropertyName());
                outputFile.write(i.getPropertyValue());
            }
        }
        outputFile.write("\n");
    }
}
