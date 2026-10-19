package fr.migration;

import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.io.*;
import java.util.stream.Collectors;

public final class MigrationService {
    private final Config cfg;
    public MigrationService(Config cfg) { this.cfg = cfg; }
    private static String seconds(long nanos) { return String.format(Locale.ROOT,"%.3f",nanos/1_000_000_000.0); }
    private static String duration(long start) { Duration d=Duration.ofNanos(System.nanoTime()-start); long ms=d.toMillis(); return String.format(Locale.ROOT,"%02d:%02d:%02d.%03d",ms/3600000,(ms/60000)%60,(ms/1000)%60,ms%1000); }
    private static String q(String s) { if(!s.matches("[A-Za-z_][A-Za-z0-9_$#]*")) throw new IllegalArgumentException("Invalid identifier: "+s); return s; }
    private static String csv(Object... values) { return Arrays.stream(values).map(v->"\""+String.valueOf(v).replace("\"","\"\"")+"\"").collect(Collectors.joining(",")); }

    public void check(List<TableConfig> tables) {
        System.out.println("CHECK: "+tables.size()+" table(s) defined in the 9-column mapping CSV.");
        for(TableConfig t:tables) {
            System.out.printf("CHECK %s -> %s (%d mapped columns)%n",t.sourceQualifiedName(),t.targetQualifiedName(),t.columns().size());
            for(ColumnMapping m:t.columns()) System.out.printf("  %s [%s] -> %s [%s]%n",m.sourceColumn(),m.sourceType(),m.targetColumn(),m.targetType());
        }
        System.out.println("Note: CHECK validates mapping structure only, not live database columns.");
    }

    public void migrate(List<TableConfig> tables) throws Exception {
        Files.createDirectories(Path.of("logs"));
        long globalStart=System.nanoTime(), total=0; int success=0, failed=0;
        Path log=Path.of("logs","migration-tables-summary.csv");
        // Recreate the per-run summary so the header always matches the current columns.
        try(BufferedWriter out=Files.newBufferedWriter(log,StandardCharsets.UTF_8,StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING);
            Connection source=Db.openSybase(cfg)) {
            out.write("source,source_count,rows_read,rows_committed,estimated_remaining,read_seconds,insert_seconds,commit_seconds,duration,status,error\n");
            out.flush();
            ConnectionHolder holder = new ConnectionHolder(RetryPolicy.openOracleWithRetry(cfg));
            holder.connection.setAutoCommit(false);
            try {
                for(TableConfig t:tables) {
                    long started=System.nanoTime(), committed=0, readRows=0, readNanos=0, insertNanos=0, commitNanos=0;
                    long sourceCount=-1;
                    String status="SUCCESS",error="";
                    try {
                        source.setCatalog(t.sourceCatalog());
                        sourceCount=Long.parseLong(countRows(source, q(t.sourceSchema())+"."+q(t.sourceTable())));
                        System.out.printf("SOURCE COUNT %s | count(*)=%d%n",t.sourceQualifiedName(),sourceCount);
                        String columns=t.columns().stream().map(m->q(m.sourceColumn())).collect(Collectors.joining(","));
                        String targets=t.columns().stream().map(m->q(m.targetColumn())).collect(Collectors.joining(","));
                        String marks=String.join(",",Collections.nCopies(t.columns().size(),"?"));
                        String select="SELECT "+columns+" FROM "+q(t.sourceSchema())+"."+q(t.sourceTable());
                        String insert="INSERT INTO "+q(t.targetSchema())+"."+q(t.targetTable())+" ("+targets+") VALUES ("+marks+")";
                        System.out.println("START "+t.sourceQualifiedName()+" -> "+t.targetQualifiedName());
                        try(PreparedStatement reader=source.prepareStatement(select,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY)) {
                            reader.setFetchSize(Math.max(1,cfg.batchSize()));
                            int batchSize=Math.max(1,cfg.oracleJdbcBatchSize());
                            List<Object[]> pending=new ArrayList<>(batchSize);
                            long nextProgressAt=10000;
                            try(ResultSet rs=reader.executeQuery()) {
                                while(true) {
                                    long n=System.nanoTime(); boolean hasRow=rs.next(); readNanos+=System.nanoTime()-n;
                                    if(!hasRow) break;
                                    Object[] row=new Object[t.columns().size()];
                                    for(int i=0;i<t.columns().size();i++) {
                                        try {
                                            row[i]=ValueBinder.readValue(rs,t.columns().get(i));
                                        } catch (Exception conversionError) {
                                            logReadConversionFailure(t, rs, row, i, conversionError);
                                            throw conversionError;
                                        }
                                    }
                                    pending.add(row); readRows++;
                                    if(pending.size()>=batchSize) {
                                        long ins=System.nanoTime(); executeBatchWithRetry(holder,insert,t,pending); insertNanos+=System.nanoTime()-ins;
                                        long cs=System.nanoTime(); commitWithNoReplay(holder); commitNanos+=System.nanoTime()-cs;
                                        committed+=pending.size(); pending.clear();
                                        if(committed>=nextProgressAt) {
                                            System.out.printf("PROGRESS %s | read=%d | committed=%d | elapsed=%s | rs.next=%ss | insertBatch=%ss | commit=%ss%n",t.sourceTable(),readRows,committed,duration(started),seconds(readNanos),seconds(insertNanos),seconds(commitNanos));
                                            nextProgressAt=((committed/10000)+1)*10000;
                                        }
                                    }
                                }
                            }
                            if(!pending.isEmpty()) {
                                long ins=System.nanoTime(); executeBatchWithRetry(holder,insert,t,pending); insertNanos+=System.nanoTime()-ins;
                                long cs=System.nanoTime(); commitWithNoReplay(holder); commitNanos+=System.nanoTime()-cs;
                                committed+=pending.size(); pending.clear();
                            }
                        }
                        success++; total+=committed;
                        System.out.printf("TABLE DONE %s | read=%d | committed=%d | readNext=%ss | insert=%ss | commit=%ss | duration=%s%n",t.sourceTable(),readRows,committed,seconds(readNanos),seconds(insertNanos),seconds(commitNanos),duration(started));
                    } catch(Exception e) {
                        failed++;
                        status="FAILED";
                        error=e.toString();
                        try {
                            holder.connection.rollback();
                        } catch(SQLException rollback) {
                            e.addSuppressed(rollback);
                        }
                        logDetailedFailure(t, readRows, committed, duration(started), e);
                    } finally {
                        String estimatedRemaining = sourceCount >= 0 ? Long.toString(Math.max(0L, sourceCount - committed)) : "";
                        out.write(csv(t.sourceQualifiedName(),sourceCount >= 0 ? sourceCount : "",readRows,committed,estimatedRemaining,seconds(readNanos),seconds(insertNanos),seconds(commitNanos),duration(started),status,error)+"\n");out.flush();
                    }
                }
                writeRowCountReport(tables, source, holder);
            } finally { try { holder.connection.close(); } catch(SQLException ignored) {} }
        }
        String summary=String.format(Locale.ROOT,"RUN SUMMARY tables=%d success=%d failed=%d rows_committed_in_successful_tables=%d duration=%s",tables.size(),success,failed,total,duration(globalStart));
        System.out.println(summary);
        Files.writeString(Path.of("logs","migration-run-summary.txt"),summary+System.lineSeparator(),StandardCharsets.UTF_8,StandardOpenOption.CREATE,StandardOpenOption.APPEND);
        if(failed>0) throw new IllegalStateException(failed+" table(s) failed. See logs/migration-tables-summary.csv");
    }


    /** Prints the complete exception chain, JDBC next-exceptions and batch update counts. */
    private void logDetailedFailure(TableConfig table, long readRows, long committed,
                                    String elapsed, Exception error) {
        StringBuilder details = new StringBuilder(2048);
        details.append(System.lineSeparator())
               .append("========== TABLE FAILED - DETAILS ==========\n")
               .append("Source      : ").append(table.sourceQualifiedName()).append('\n')
               .append("Target      : ").append(table.targetQualifiedName()).append('\n')
               .append("Rows read   : ").append(readRows).append('\n')
               .append("Rows committed before failure: ").append(committed).append('\n')
               .append("Elapsed     : ").append(elapsed).append('\n')
               .append("Top error   : ").append(error).append('\n');

        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        int depth = 0;
        while (current != null && visited.add(current)) {
            details.append("CAUSE[").append(depth++).append("]: ")
                   .append(current.getClass().getName()).append(": ")
                   .append(current.getMessage()).append('\n');

            if (current instanceof SQLException sql) {
                details.append("  SQLState  : ").append(sql.getSQLState()).append('\n')
                       .append("  ErrorCode : ").append(sql.getErrorCode()).append('\n');
                SQLException next = sql.getNextException();
                while (next != null && visited.add(next)) {
                    details.append("  NEXT SQL EXCEPTION: ")
                           .append(next.getClass().getName()).append(": ")
                           .append(next.getMessage()).append('\n')
                           .append("    SQLState  : ").append(next.getSQLState()).append('\n')
                           .append("    ErrorCode : ").append(next.getErrorCode()).append('\n');
                    appendBatchCounts(details, next);
                    next = next.getNextException();
                }
            }
            appendBatchCounts(details, current);
            current = current.getCause();
        }

        java.io.StringWriter sw = new java.io.StringWriter();
        error.printStackTrace(new java.io.PrintWriter(sw));
        details.append("STACK TRACE:\n").append(sw);
        details.append("========== END TABLE FAILURE DETAILS ==========\n");

        String message = details.toString();
        System.err.print(message);
        try {
            Files.createDirectories(Path.of("logs"));
            Files.writeString(Path.of("logs", "migration-errors.log"), message,
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException logFailure) {
            System.err.println("Could not write logs/migration-errors.log: " + logFailure.getMessage());
        }
    }

    private static void appendBatchCounts(StringBuilder details, Throwable error) {
        if (error instanceof BatchUpdateException batch) {
            int[] counts = batch.getUpdateCounts();
            details.append("  BATCH UPDATE COUNTS: ").append(counts.length)
                   .append(" result(s); values=").append(Arrays.toString(counts)).append('\n');
        }
    }

    /** Retries only executeBatch failures caused by a broken connection. Since commit has not been requested yet, a disconnected Oracle session rolls back its uncommitted batch. */
    private void executeBatchWithRetry(ConnectionHolder holder, String insert, TableConfig table, List<Object[]> rows) throws SQLException {
        SQLException last=null;
        for(int attempt=0; attempt<=RetryPolicy.MAX_RETRIES; attempt++) {
            try(PreparedStatement ps=holder.connection.prepareStatement(insert)) {
                for(Object[] row:rows) {
                    for(int i=0;i<row.length;i++) ValueBinder.bindValue(row[i],ps,i+1,table.columns().get(i));
                    ps.addBatch();
                }
                ps.executeBatch();
                return;
            } catch(SQLException e) {
                last=e;
                if(!RetryPolicy.isConnectionFailure(e) || attempt==RetryPolicy.MAX_RETRIES) break;
                System.err.printf("Oracle connection lost during executeBatch for %s (retry %d/%d). Waiting 10 seconds; replaying %d uncommitted rows after reconnect.%n",table.sourceTable(),attempt+1,RetryPolicy.MAX_RETRIES,rows.size());
                sleep10(e);
                replaceConnection(holder,e);
            }
        }
        logFailedInsertRows(insert, table, rows, last);
        throw new SQLException("Oracle executeBatch failed after initial attempt + retries. Table="+table.sourceTable(),last);
    }

    /** Writes executable-looking INSERT statements with the actual values of every row in the failed batch. */
    private void logFailedInsertRows(String insertTemplate, TableConfig table, List<Object[]> rows, SQLException error) {
        Path file = Path.of("logs", "failed-inserts.sql");
        try {
            Files.createDirectories(file.getParent());
            try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                out.write("-- FAILED INSERT BATCH; source=" + table.sourceQualifiedName()
                        + "; target=" + table.targetQualifiedName() + "; error="
                        + sqlComment(String.valueOf(error.getMessage())) + "\n");
                out.write("-- Prepared SQL template: " + insertTemplate + "\n");
                out.write("-- The following statements use target-column types and actual values from the failed batch.\n");
                int rowNumber = 0;
                for (Object[] row : rows) {
                    rowNumber++;
                    out.write("-- failed batch row " + rowNumber + "\n");
                    out.write(renderInsert(table, row));
                    out.write(";\n");
                }
                out.write("-- END FAILED INSERT BATCH\n\n");
            }
            System.err.println("Failed INSERT values written to " + file.toAbsolutePath());
        } catch (Exception loggingError) {
            System.err.println("Unable to write failed INSERT values: " + loggingError.getMessage());
            error.addSuppressed(loggingError);
        }
    }

    private String renderInsert(TableConfig table, Object[] row) {
        String columns = table.columns().stream().map(m -> q(m.targetColumn())).collect(Collectors.joining(", "));
        List<String> literals = new ArrayList<>(row.length);
        for (int i = 0; i < row.length; i++) literals.add(sqlLiteral(row[i], table.columns().get(i)));
        return "INSERT INTO " + q(table.targetSchema()) + "." + q(table.targetTable())
                + " (" + columns + ") VALUES (" + String.join(", ", literals) + ")";
    }

    private String sqlLiteral(Object value, ColumnMapping mapping) {
        if (value == null) return "NULL";
        String target = TypeMapper.normalize(mapping.targetType());
        if (value instanceof byte[] bytes) {
            String hex = HexFormat.of().formatHex(bytes).toUpperCase(Locale.ROOT);
            if (target.equals("blob")) return "TO_BLOB(HEXTORAW('" + hex + "'))";
            return "HEXTORAW('" + hex + "')";
        }
        if (value instanceof Timestamp ts) {
            String text = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
                    .format(ts.toLocalDateTime());
            return "TO_TIMESTAMP('" + text + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
        }
        if (value instanceof java.sql.Date date) {
            return "TO_DATE('" + date.toLocalDate() + "', 'YYYY-MM-DD')";
        }
        if (value instanceof Time time) {
            return "'" + time.toString().replace("'", "''") + "'";
        }
        if (value instanceof Number) return value.toString();
        if (value instanceof Boolean b) return b ? "1" : "0";
        String text = String.valueOf(value).replace("'", "''");
        if (target.equals("clob") || target.equals("nclob")) return "TO_CLOB('" + text + "')";
        return "'" + text + "'";
    }

    private void logReadConversionFailure(TableConfig table, ResultSet rs, Object[] partiallyReadRow,
                                          int failedColumnIndex, Exception error) {
        Object[] diagnosticRow = Arrays.copyOf(partiallyReadRow, partiallyReadRow.length);
        for (int i = 0; i < diagnosticRow.length; i++) {
            if (i == failedColumnIndex || diagnosticRow[i] == null) {
                try { diagnosticRow[i] = rs.getObject(table.columns().get(i).sourceColumn()); }
                catch (SQLException ignored) { diagnosticRow[i] = "<unreadable source value>"; }
            }
        }
        Path file = Path.of("logs", "failed-inserts.sql");
        try {
            Files.createDirectories(file.getParent());
            try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                out.write("-- FAILURE WHILE READING/TRANSFORMING SOURCE ROW; source=" + table.sourceQualifiedName()
                        + "; target=" + table.targetQualifiedName() + "; column="
                        + table.columns().get(failedColumnIndex).sourceColumn() + "; error="
                        + sqlComment(String.valueOf(error.getMessage())) + "\n");
                out.write("-- Diagnostic INSERT: some values may be raw source values because conversion failed.\n");
                out.write(renderInsert(table, diagnosticRow) + ";\n\n");
            }
        } catch (Exception loggingError) {
            System.err.println("Unable to log source-row conversion failure: " + loggingError.getMessage());
            error.addSuppressed(loggingError);
        }
    }

    private static String sqlComment(String text) {
        return text.replace("\r", " ").replace("\n", " ").replace("--", "- -");
    }

    /** Creates the requested final source/target COUNT(*) comparison, including tables that failed migration. */
    private void writeRowCountReport(List<TableConfig> tables, Connection source, ConnectionHolder holder) {
        Path file = Path.of("logs", "migration-row-counts.csv");
        List<String[]> results = new ArrayList<>();
        for (TableConfig table : tables) {
            String sourceCount = "", targetCount = "", status = "OK", error = "";
            try {
                source.setCatalog(table.sourceCatalog());
                sourceCount = countRows(source, q(table.sourceSchema()) + "." + q(table.sourceTable()));
            } catch (Exception e) {
                status = "SOURCE_COUNT_FAILED";
                error = e.toString();
            }
            try {
                targetCount = countRows(holder.connection, q(table.targetSchema()) + "." + q(table.targetTable()));
            } catch (Exception e) {
                status = status.equals("OK") ? "TARGET_COUNT_FAILED" : status + ";TARGET_COUNT_FAILED";
                error = error.isEmpty() ? e.toString() : error + " | " + e;
            }
            String difference = "";
            try { if (!sourceCount.isEmpty() && !targetCount.isEmpty()) difference = Long.toString(Long.parseLong(sourceCount) - Long.parseLong(targetCount)); }
            catch (NumberFormatException ignored) { }
            results.add(new String[]{table.sourceQualifiedName(), sourceCount, table.targetQualifiedName(), targetCount, difference, status, error});
        }
        try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            out.write("source_table,source_count,target_table,target_count,source_minus_target,status,error\n");
            for (String[] row : results) out.write(csv((Object[]) row) + "\n");
        } catch (IOException e) {
            System.err.println("Unable to write row-count report: " + e.getMessage());
        }
        System.out.println();
        System.out.println("FINAL ROW COUNTS (also saved to " + file + ")");
        System.out.printf("%-48s | %12s | %-48s | %12s | %10s | %s%n", "SOURCE TABLE", "COUNT(*)", "TARGET TABLE", "COUNT(*)", "DIFF", "STATUS");
        for (String[] row : results) {
            System.out.printf("%-48s | %12s | %-48s | %12s | %10s | %s%n",
                    row[0], row[1], row[2], row[3], row[4], row[5]);
        }
    }

    private static String countRows(Connection connection, String qualifiedTable) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable)) {
            if (!rs.next()) throw new SQLException("COUNT(*) returned no row for " + qualifiedTable);
            return rs.getString(1);
        }
    }

    /** A commit error is ambiguous: Oracle may have committed even if the acknowledgement was lost. Never replay that batch blindly. */
    private void commitWithNoReplay(ConnectionHolder holder) throws SQLException {
        try { holder.connection.commit(); }
        catch(SQLException e) {
            if(RetryPolicy.isConnectionFailure(e)) {
                try { replaceConnection(holder,e); }
                catch(SQLException reconnect) { e.addSuppressed(reconnect); }
                throw new SQLException("Connection lost during COMMIT. Commit outcome is uncertain; batch was NOT replayed to avoid duplicates. Reconcile target rows before resuming.",e);
            }
            throw e;
        }
    }

    private void replaceConnection(ConnectionHolder holder, SQLException cause) throws SQLException {
        try { holder.connection.close(); } catch(SQLException close) { cause.addSuppressed(close); }
        holder.connection=RetryPolicy.openOracleWithRetry(cfg);
        holder.connection.setAutoCommit(false);
    }
    private static void sleep10(SQLException original) throws SQLException {
        try { Thread.sleep(RetryPolicy.DELAY_MS); }
        catch(InterruptedException e) { Thread.currentThread().interrupt(); original.addSuppressed(e); throw new SQLException("Interrupted while waiting for Oracle retry",original); }
    }
    private static final class ConnectionHolder { private Connection connection; private ConnectionHolder(Connection c){connection=c;} }
}
