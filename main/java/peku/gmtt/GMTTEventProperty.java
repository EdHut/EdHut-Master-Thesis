package peku.gmtt;

public class GMTTEventProperty {
    private String propertyValue;
    private GMTTEventPropertyDefinition propertyType;

    public GMTTEventProperty(String propertyValue, GMTTEventPropertyDefinition propertyType) {
        this.propertyValue = propertyValue;
        this.propertyType = propertyType;
    }

    String getPropertyValue() {
        return propertyValue;
    }

    String getXESTag() {
        return propertyType.getTagXES();
    }

    String getXESKey() {
        return propertyType.getPropertyName();
    }
}
