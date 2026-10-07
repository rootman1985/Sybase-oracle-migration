package fr.migration;

import java.util.Properties;

public record Config(Properties p) {
    String sybaseUrl(){ return req("sybase.url"); }
    String sybaseUser(){ return req("sybase.user"); }
    String sybasePassword(){ return req("sybase.password"); }
    String sybaseDriver(){ return req("sybase.driver"); }
    String oracleUrl(){ return req("oracle.url"); }
    String oracleUser(){ return req("oracle.user"); }
    String oraclePassword(){ return req("oracle.password"); }
    String oracleDriver(){ return req("oracle.driver"); }
    int rangeSize(){ return Integer.parseInt(p.getProperty("migration.range-size","10000")); }
    int batchSize(){ return Integer.parseInt(p.getProperty("migration.batch-size","1000")); }
    int fetchSize(){ return Integer.parseInt(p.getProperty("migration.fetch-size","1000")); }
    int queryTimeout(){ return Integer.parseInt(p.getProperty("migration.query-timeout-seconds","0")); }
    boolean failOnIncompatible(){ return Boolean.parseBoolean(p.getProperty("migration.fail-on-incompatible","true")); }
    boolean syncIdentity(){ return Boolean.parseBoolean(p.getProperty("migration.sync-identity","false")); }
    String stateDir(){ return p.getProperty("migration.state-directory","state"); }
    String errorDir(){ return p.getProperty("migration.error-directory","logs"); }
    private String req(String k){ String v=p.getProperty(k); if(v==null || v.isBlank()) throw new IllegalArgumentException("Propriété manquante: "+k); return v; }
}