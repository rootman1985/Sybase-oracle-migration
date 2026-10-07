package fr.migration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.OffsetDateTime;

public class ErrorLogger {
    private final Path file;
    public ErrorLogger(String dir,String table)throws IOException{
        Files.createDirectories(Path.of(dir));
        file=Path.of(dir,table.replaceAll("[^A-Za-z0-9_.-]","_")+"_errors.csv");
        if(!Files.exists(file)) Files.writeString(file,"timestamp,table,error\n",StandardCharsets.UTF_8);
    }
    public synchronized void log(String table,String error)throws IOException{
        String e=error.replace("\"","\"\"");
        String line="\""+OffsetDateTime.now()+"\",\""+table+"\",\""+e+"\"\n";
        Files.writeString(file,line,StandardCharsets.UTF_8,StandardOpenOption.CREATE,StandardOpenOption.APPEND);
    }
}