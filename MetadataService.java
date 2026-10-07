package fr.migration;

import java.sql.*;
import java.util.*;

public class MetadataService {
    public List<ColumnInfo> columns(Connection c, String schema, String table) throws SQLException {
        List<ColumnInfo> result = new ArrayList<>();
        DatabaseMetaData md = c.getMetaData();
        read(md, schema, table, result);
        if(result.isEmpty()) read(md, schema == null ? null : schema.toUpperCase(Locale.ROOT),
                table == null ? null : table.toUpperCase(Locale.ROOT), result);
        result.sort(Comparator.comparingInt(ColumnInfo::ordinal));
        if(result.isEmpty()) throw new SQLException("Table introuvable ou metadata vide: "+schema+"."+table);
        return result;
    }
    private void read(DatabaseMetaData md,String schema,String table,List<ColumnInfo> result)throws SQLException{
        try(ResultSet rs=md.getColumns(null,schema,table,"%")){
            while(rs.next()){
                result.add(new ColumnInfo(
                    rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME"),
                    rs.getInt("COLUMN_SIZE"), rs.getInt("DECIMAL_DIGITS"), rs.getInt("ORDINAL_POSITION"),
                    "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")),
                    rs.getString("COLUMN_DEF"),
                    "YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT"))));
            }
        }
    }
}