package fr.migration;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CsvConfigLoader {
    public List<TableConfig> loadTables(InputStream in) throws IOException {
        if (in == null) throw new FileNotFoundException("tables.csv");
        List<TableConfig> out = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            r.readLine();
            String line;
            while ((line=r.readLine()) != null) {
                if (line.isBlank() || line.trim().startsWith("#")) continue;
                String[] x = split(line);
                if (x.length < 6) throw new IllegalArgumentException("Ligne tables.csv invalide: "+line);
                out.add(new TableConfig(x[0],x[1],x[2],x[3],blankToNull(x[4]),Boolean.parseBoolean(x[5])));
            }
        }
        return out;
    }
    public List<ColumnMapping> loadMappings(InputStream in) throws IOException {
        if (in == null) return List.of();
        List<ColumnMapping> out = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            r.readLine();
            String line;
            while ((line=r.readLine()) != null) {
                if (line.isBlank() || line.trim().startsWith("#")) continue;
                String[] x=split(line);
                if(x.length>=3) out.add(new ColumnMapping(x[0],x[1],x[2]));
            }
        }
        return out;
    }
    private static String blankToNull(String s){ return s==null || s.isBlank()?null:s.trim(); }
    private static String[] split(String s){ return s.split(",",-1); }
}