package fr.migration;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;

public final class ValueBinder {
    public static void bind(PreparedStatement ps,int index,ResultSet rs,int sourceType,int targetType)throws SQLException {
        if(rs.getObject(index)==null){ ps.setNull(index,targetType); return; }
        Object v;
        if(sourceType==Types.LONGVARCHAR || sourceType==Types.CLOB || sourceType==Types.VARCHAR || sourceType==Types.CHAR)
            v=rs.getString(index);
        else if(sourceType==Types.LONGVARBINARY || sourceType==Types.BINARY || sourceType==Types.VARBINARY || sourceType==Types.BLOB)
            v=rs.getBytes(index);
        else if(sourceType==Types.DATE)
            v=rs.getDate(index);
        else if(sourceType==Types.TIME)
            v=rs.getTime(index);
        else if(sourceType==Types.TIMESTAMP)
            v=rs.getTimestamp(index);
        else if(sourceType==Types.NUMERIC || sourceType==Types.DECIMAL)
            v=rs.getBigDecimal(index);
        else
            v=rs.getObject(index);

        if(targetType==Types.CLOB && v instanceof String s){
            ps.setCharacterStream(index,new StringReader(s),s.length());
        } else if(targetType==Types.BLOB && v instanceof byte[] b){
            ps.setBinaryStream(index,new ByteArrayInputStream(b),b.length);
        } else if(targetType==Types.NUMERIC || targetType==Types.DECIMAL){
            if(v instanceof Number n && !(v instanceof BigDecimal)) ps.setBigDecimal(index,new BigDecimal(n.toString()));
            else ps.setObject(index,v,targetType);
        } else if(targetType==Types.TIMESTAMP && v instanceof java.util.Date d){
            ps.setTimestamp(index,new Timestamp(d.getTime()));
        } else {
            ps.setObject(index,v,targetType);
        }
    }
}