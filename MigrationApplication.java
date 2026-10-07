package fr.migration;

import java.io.InputStream;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;

public class MigrationApplication {
    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0].toUpperCase() : "CHECK";
        String onlyTable = args.length > 1 ? args[1] : null;

        Properties p = new Properties();
        try (InputStream in = MigrationApplication.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in == null) throw new IllegalStateException("application.properties introuvable");
            p.load(in);
        }

        Config config = new Config(p);
        CsvConfigLoader loader = new CsvConfigLoader();
        List<TableConfig> tables = loader.loadTables(
                MigrationApplication.class.getClassLoader().getResourceAsStream("tables.csv"));
        List<ColumnMapping> mappings = loader.loadMappings(
                MigrationApplication.class.getClassLoader().getResourceAsStream("column-mappings.csv"));

        if (onlyTable != null) {
            tables = tables.stream()
                    .filter(t -> t.sourceTable().equalsIgnoreCase(onlyTable)
                            || t.targetTable().equalsIgnoreCase(onlyTable))
                    .toList();
        }

        try (Connection sybase = Db.open(config.sybaseUrl(), config.sybaseUser(), config.sybasePassword(), config.sybaseDriver());
             Connection oracle = Db.open(config.oracleUrl(), config.oracleUser(), config.oraclePassword(), config.oracleDriver())) {

            sybase.setReadOnly(true);
            oracle.setAutoCommit(false);

            MetadataService metadata = new MetadataService();
            MigrationService service = new MigrationService(config, metadata, mappings);

            for (TableConfig table : tables) {
                if (!table.enabled()) continue;
                if ("CHECK".equals(mode)) {
                    service.check(sybase, oracle, table);
                } else if ("MIGRATE".equals(mode)) {
                    service.migrate(sybase, oracle, table);
                } else {
                    throw new IllegalArgumentException("Mode attendu: CHECK ou MIGRATE");
                }
            }
        }
    }
}