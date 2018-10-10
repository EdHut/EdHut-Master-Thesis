package peku.gmtt;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

class EventSortByTime implements Comparator<GMTTEvent>
{
    // used for sorting events in chronological order
    public int compare(GMTTEvent a, GMTTEvent b) {
        if (a.getComponentStatusDateTime() < b.getComponentStatusDateTime()) {
            return -1;
        } {
            return 1;
        }
    }
}

public class GMTTComponent {
    private UUID ComponentID;
    private UUID ParentComponentID;

    private Set<GMTTEvent> Events = new HashSet<>();
    private Set<GMTTComponent> Components = new HashSet<>();

    private GMTTComponent parentComponent;  // the parentComponent, if the component has one.

    private Long firstChildTimestamp, lastChildTimestamp;

    private boolean suitableForParent = true;   // will be put to false in case too many children components are added
    private boolean isHighestComponent = true;  // consider new component to be highest in hierarchy, unless proven it's not.
                                                // note that even if it has a parentcomponent, it my still be highest, because
                                                // its parent has more children than we want to consider

    public GMTTComponent(UUID componentID, UUID parentComponentID) {
        ComponentID = componentID;
        ParentComponentID = parentComponentID;
    }

    public GMTTComponent(UUID componentID) {
        ComponentID = componentID;
    }

    // Add a Component to the Component (which is thus a Parent)
    public boolean addComponentToParentComponent(GMTTComponent component, Long eventTimestamp) {
//        if (suitableForParent) {
            boolean result = Components.add(component);

            if (Components.size() == 1) {
                // record the timestamp of the first found child
                firstChildTimestamp = eventTimestamp;

            } else if (Components.size() == 3) {    // max size arbitrarily chosen at design time
                // Too many children results in unrealistically large traces (which we don't want)
                lastChildTimestamp = eventTimestamp;
                suitableForParent = false;

                // each child component will be marked as highest in hierarchy again
                for (GMTTComponent childComponent : Components) {
                    childComponent.setHighestComponent(true);
                }
//                System.out.println("Parent:" + ComponentID + " Unsuitable as parent after (msec):" + getTimeSpentUnsuitableParent());
            }
            return result;
    }

    public boolean isParent() {
        return (suitableForParent & (Components.size() > 0));
    }

    public boolean isSuitableForParent() { return suitableForParent; }

    public boolean AddEventToComponent(GMTTEvent event) {
        return Events.add(event);
    }

    public UUID getParentComponentID() {
        return ParentComponentID;
    }

    public void setParentComponentID(UUID parentComponentID) {
        ParentComponentID = parentComponentID;
    }

    public UUID getComponentID() {
        return ComponentID;
    }

    public void setComponentID(UUID componentID) {
        ComponentID = componentID;
    }

    public Long getTimeSpentUnsuitableParent() {
        // returns the number of msec before a parent got too many children te remain a sensible parent for sake of process mining
        if (!suitableForParent) {
            return lastChildTimestamp - firstChildTimestamp;
        } else {
            return null;
        }
    }

    public void writeXESoutput (BufferedWriter outputFile) throws IOException {
        //outputFile.write ("Component:" + ComponentID);

        if (isHighestComponent & (parentComponent != null)) {
            // A highestComponent may still have a parent, e.g. when it is a transaction within a large payment order (MECT)
            // For such cases the events of the (e.g.) MECT order and possibly also the file in which it was received are
            // also added to the trace of the transaction.
            parentComponent.writeXESoutputOnlyParent(outputFile);
        }

        if (suitableForParent) {
            for (Iterator<GMTTComponent> itComponent = Components.iterator(); itComponent.hasNext();) {
            GMTTComponent component = itComponent.next();
            //Note: recursive call to write the output
                component.writeXESoutput(outputFile);
            }
        }
        for (Iterator<GMTTEvent> itEvent = Events.iterator(); itEvent.hasNext();) {
            GMTTEvent value = itEvent.next();
            //outputFile.write("Event:" + value.getComponentStatus());
            value.writeXESoutput(outputFile);
        }
    }

    public void writeXESoutputOnlyParent(BufferedWriter outputFile) throws IOException {

        if (parentComponent != null) {
            parentComponent.writeXESoutputOnlyParent(outputFile);
        }

        for (Iterator<GMTTEvent> itEvent = Events.iterator(); itEvent.hasNext();) {
            GMTTEvent value = itEvent.next();
            //outputFile.write("Event:" + value.getComponentStatus());
            value.writeXESoutput(outputFile);
        }
    }

    public void writeCSVoutput (BufferedWriter outputFile, UUID traceID, Map<String, GMTTEventPropertyDefinition> includeProperties) throws IOException {

        ArrayList<GMTTEvent> listEvents = new ArrayList<GMTTEvent>();

        if (isHighestComponent & (parentComponent != null)) {
            // A highestComponent may still have a parent, e.g. when it is a transaction within a large payment order (MECT)
            // For such cases the events of the (e.g.) MECT order and possibly also the file in which it was received are
            // also added to the trace of the transaction.
            parentComponent.CollectOnlyParent(listEvents);
        }

        // All components are traversed and of each component the events are add to the list of events
        this.collectComponentEvents(listEvents);

        // Sort the events on timestamp
        listEvents.sort(new EventSortByTime());

        for (Iterator<GMTTEvent> itEvent = listEvents.iterator(); itEvent.hasNext();) {
            GMTTEvent value = itEvent.next();
            value.writeCSVoutput(outputFile, traceID, includeProperties);
        }
    }

    public void writeStateTransitions (BufferedWriter outputFile, UUID traceID, Map<String, GMTTEventPropertyDefinition> includeProperties) throws IOException {

        ArrayList<GMTTEvent> listEvents = new ArrayList<GMTTEvent>();

        if (isHighestComponent & (parentComponent != null)) {
            // A highestComponent may still have a parent, e.g. when it is a transaction within a large payment order (MECT)
            // For such cases the events of the (e.g.) MECT order and possibly also the file in which it was received are
            // also added to the trace of the transaction.
            parentComponent.CollectOnlyParent(listEvents);
        }

        // All components are traversed and of each component the events are add to the list of events
        this.collectComponentEvents(listEvents);

        // Sort the events on timestamp
        listEvents.sort(new EventSortByTime());

        GMTTEvent GMTTEventPrevious = null;
        for (Iterator<GMTTEvent> itEvent = listEvents.iterator(); itEvent.hasNext();) {
            GMTTEvent value = itEvent.next();
            value.writeStateTransitionOutput(outputFile, traceID, includeProperties, GMTTEventPrevious);
            GMTTEventPrevious = value;
        }
        if (GMTTEventPrevious != null) {
            GMTTEventPrevious.writeStateTransitionOutputAsFinalstatus(outputFile, traceID, includeProperties);
        }
    }

    // method that can be invoked recursively to add all events of a trace to a list
    public void collectComponentEvents (ArrayList<GMTTEvent> listEvents) {

        if (suitableForParent) {
            for (Iterator<GMTTComponent> itComponent = Components.iterator(); itComponent.hasNext();) {
                GMTTComponent component = itComponent.next();
                //Note: recursive call to traverse the hierarchy of components
                component.collectComponentEvents(listEvents);
            }
        }
        for (Iterator<GMTTEvent> itEvent = Events.iterator(); itEvent.hasNext();) {
            listEvents.add(itEvent.next());
        }
    }

    public void writeCSVoutputOnlyParent(BufferedWriter outputFile, UUID traceID, Map<String, GMTTEventPropertyDefinition> includeProperties) throws IOException {

        if (parentComponent != null) {
            parentComponent.writeCSVoutputOnlyParent(outputFile, traceID, includeProperties);
        }

        for (Iterator<GMTTEvent> itEvent = Events.iterator(); itEvent.hasNext();) {
            GMTTEvent value = itEvent.next();
            //outputFile.write("Event:" + value.getComponentStatus());
            value.writeCSVoutput(outputFile, traceID, includeProperties);
        }
    }

    // Method to add events of a parent component to the ArrayList of events
    public void CollectOnlyParent(ArrayList<GMTTEvent> listEvents)  {
        if (parentComponent != null) {
            parentComponent.CollectOnlyParent(listEvents);
        }

        for (Iterator<GMTTEvent> itEvent = Events.iterator(); itEvent.hasNext();) {
            GMTTEvent value = itEvent.next();
            //outputFile.write("Event:" + value.getComponentStatus());
            listEvents.add(value);
        }
    }


    public int getNumberOfChildren () {
        int i = 0;

        if (suitableForParent) {
            for (Iterator<GMTTComponent> itComponent = Components.iterator(); itComponent.hasNext();) {
                GMTTComponent component = itComponent.next();
                i += component.getNumberOfChildren();   // recursive call
            }
        }
        i += Events.size();
        return i;
    }

    public int getNumberOfDirectChildren () {
        return Components.size();
    }

    public boolean isHighestComponent() {
        return isHighestComponent;
    }

    public void setHighestComponent(boolean highestComponent) {
        isHighestComponent = highestComponent;
    }

    public GMTTComponent getParentComponent() {
        return parentComponent;
    }

    public void setParentComponent(GMTTComponent parentComponent) {
        this.parentComponent = parentComponent;
    }
}
