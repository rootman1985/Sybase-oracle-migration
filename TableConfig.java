package fr.migration;
public record TableConfig(String sourceSchema, String sourceTable, String targetSchema,
                          String targetTable, String idColumn, boolean enabled) {}