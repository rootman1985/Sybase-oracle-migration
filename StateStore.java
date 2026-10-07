package fr.migration;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.*;

public class StateStore {
    private final Path dir;
    public StateStore(String d){dir=Path.of(d);}
    public BigDecimal load(String table){ try{
        Path p=dir.resolve(safe(table)+".state");
        if(!Files.exists(p)) return BigDecimal.ZERO;
        return new BigDecimal(Files.readString(p).trim());
    }catch(Exception e){throw new RuntimeException(e);}
    }
    public void save(String table, BigDecimal id){ try{
        Files.createDirectories(dir);
        Path tmp=dir.resolve(safe(table)+".state.tmp");
        Files.writeString(tmp,id.toPlainString(),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmp,dir.resolve(safe(table)+".state"),StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);
    }catch(Exception e){throw new RuntimeException(e);}
    }
    private static String safe(String s){return s.replaceAll("[^A-Za-z0-9_.-]","_");}
}