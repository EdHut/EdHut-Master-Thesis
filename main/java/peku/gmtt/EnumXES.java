package peku.gmtt;

public enum EnumXES {
    XES_STRING("string"),
    XES_DATE("date");

    private String XES_Tag;

    EnumXES(String XES_Tag) {
        this.XES_Tag = XES_Tag;
    }

    public String XES_tag() {
        return XES_Tag;
    }
}
