package peku.gmtt;

public class GMTTEventPropertyDefinition {
    private String propertyName;
    private String tagXES;      // which XML tag name to use?
    private String propertyNameCSV;

    public GMTTEventPropertyDefinition(String propertyName, String tagXES, String propertyNameCSV) {
        this.propertyName = propertyName;
        this.tagXES = tagXES;
        this.propertyNameCSV = propertyNameCSV;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getTagXES() {
        return tagXES;
    }

    public String getPropertyNameCSV() {
        return propertyNameCSV;
    }
}
