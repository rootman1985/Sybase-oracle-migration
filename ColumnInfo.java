package fr.migration;

public record ColumnInfo(String name, int jdbcType, String typeName, int size, int scale,
                         int ordinal, boolean nullable, String defaultValue, boolean autoIncrement) {
    @Override public String toString() {
        return name + " " + typeName + "(" + size + "," + scale + ")" +
                (autoIncrement ? " IDENTITY" : "");
    }
}