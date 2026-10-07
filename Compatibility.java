package fr.migration;

import java.sql.Types;
import java.util.*;

public class Compatibility {
    public static List<String> check(List<ColumnInfo> src, List<ColumnInfo> dst,
                                      TableConfig table, List<ColumnMapping> mappings) {
        Map<String,ColumnInfo> d = new HashMap<>();
        for(ColumnInfo c:dst) d.put(norm(c.name()),c);
        Set<String> sourceTargets = new HashSet<>();
        List<String> problems=new ArrayList<>();

        for(ColumnInfo s:src){
            String targetName=targetName(table.sourceTable(),s.name(),mappings);
            sourceTargets.add(norm(targetName));
            ColumnInfo t=d.get(norm(targetName));
            if(t==null){ problems.add("ERREUR: colonne source absente Oracle: "+s.name()+" -> "+targetName); continue; }
            if(!compatible(s,t)) problems.add("ERREUR: type incompatible: "+s+" -> "+t);
            if(isText(s.jdbcType()) && isText(t.jdbcType()) && t.size()>0 && s.size()>0 && s.size()>t.size())
                problems.add("ERREUR: taille source > destination: "+s.name()+" "+s.size()+" > "+t.size());
            if(isNum(s.jdbcType()) && isNum(t.jdbcType()) && s.scale()>t.scale() && t.scale()>=0)
                problems.add("ATTENTION: précision décimale potentiellement perdue: "+s.name()+" scale "+s.scale()+" -> "+t.scale());
        }

        for(ColumnInfo t:dst){
            if(sourceTargets.contains(norm(t.name()))) continue;
            if(t.autoIncrement() || t.defaultValue()!=null || t.nullable()) continue;
            problems.add("ERREUR: colonne Oracle obligatoire absente de la source: "+t.name());
        }
        return problems;
    }
    public static String targetName(String sourceTable,String sourceColumn,List<ColumnMapping> m){
        return m.stream().filter(x->x.sourceTable().equalsIgnoreCase(sourceTable)&&x.sourceColumn().equalsIgnoreCase(sourceColumn))
                .map(ColumnMapping::targetColumn).findFirst().orElse(sourceColumn);
    }
    static String norm(String s){return s.replace("_","").toUpperCase(Locale.ROOT);}
    static boolean isText(int t){return t==Types.VARCHAR||t==Types.CHAR||t==Types.LONGVARCHAR||t==Types.NVARCHAR||t==Types.NCHAR||t==Types.LONGNVARCHAR;}
    static boolean isNum(int t){return t==Types.TINYINT||t==Types.SMALLINT||t==Types.INTEGER||t==Types.BIGINT||t==Types.NUMERIC||t==Types.DECIMAL||t==Types.FLOAT||t==Types.REAL||t==Types.DOUBLE;}
    static boolean isDate(int t){return t==Types.DATE||t==Types.TIME||t==Types.TIMESTAMP;}
    static boolean isBinary(int t){return t==Types.BINARY||t==Types.VARBINARY||t==Types.LONGVARBINARY||t==Types.BLOB;}
    static boolean compatible(ColumnInfo s,ColumnInfo t){
        if(s.jdbcType()==t.jdbcType()) return true;
        if(isNum(s.jdbcType())&&isNum(t.jdbcType())) return true;
        if(isText(s.jdbcType())&&isText(t.jdbcType())) return true;
        if(isDate(s.jdbcType())&&isDate(t.jdbcType())) return true;
        if(isBinary(s.jdbcType())&&isBinary(t.jdbcType())) return true;
        if((s.jdbcType()==Types.LONGVARCHAR)&&(t.jdbcType()==Types.CLOB)) return true;
        if((s.jdbcType()==Types.LONGVARBINARY)&&(t.jdbcType()==Types.BLOB)) return true;
        return false;
    }
}