package fr.migration;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

public class MigrationService {
    private static final Logger LOG=Logger.getLogger(MigrationService.class.getName());
    private final Config config;
    private final MetadataService metadata;
    private final List<ColumnMapping> mappings;
    private final StateStore state;
    public MigrationService(Config config, MetadataService metadata, List<ColumnMapping> mappings){
        this.config=config; this.metadata=metadata; this.mappings=mappings;
        this.state=new StateStore(config.stateDir());
    }

    public void check(Connection sybase, Connection oracle, TableConfig t) throws SQLException {
        LOG.info(() -> "CHECK "+t.sourceSchema()+"."+t.sourceTable()+" -> "+t.targetSchema()+"."+t.targetTable());
        List<ColumnInfo> s=metadata.columns(sybase,t.sourceSchema(),t.sourceTable());
        List<ColumnInfo> d=metadata.columns(oracle,t.targetSchema(),t.targetTable());
        String id=t.idColumn()!=null?t.idColumn():s.get(0).name();
        if(!s.get(0).name().equalsIgnoreCase(id))
            LOG.warning("La colonne de découpage configurée n'est pas la première colonne: "+id);
        List<String> problems=Compatibility.check(s,d,t,mappings);
        if(problems.isEmpty()) LOG.info("CHECK OK - "+s.size()+" colonnes compatibles.");
        else for(String x:problems) LOG.warning(x);
        LOG.info("Source ID: "+s.get(0)+", destination ID: "+d.get(0));
    }

    public void migrate(Connection sybase, Connection oracle, TableConfig t) throws Exception {
        List<ColumnInfo> s=metadata.columns(sybase,t.sourceSchema(),t.sourceTable());
        List<ColumnInfo> d=metadata.columns(oracle,t.targetSchema(),t.targetTable());
        String id=t.idColumn()!=null?t.idColumn():s.get(0).name();
        ColumnInfo idInfo=s.stream().filter(c->c.name().equalsIgnoreCase(id)).findFirst()
                .orElseThrow(()->new SQLException("Colonne ID introuvable: "+id));
        if(!Compatibility.isNum(idInfo.jdbcType()))
            throw new SQLException("La colonne de découpage doit être numérique: "+idInfo);

        List<String> problems=Compatibility.check(s,d,t,mappings);
        for(String p:problems) LOG.warning(p);
        if(config.failOnIncompatible() && problems.stream().anyMatch(x->x.startsWith("ERREUR:")))
            throw new SQLException("Migration arrêtée: incompatibilités détectées pour "+t.sourceTable());

        Map<String,ColumnInfo> dstByNorm=new HashMap<>();
        for(ColumnInfo c:d) dstByNorm.put(Compatibility.norm(c.name()),c);

        List<ColumnInfo> targetCols=new ArrayList<>();
        for(ColumnInfo sc:s){
            String tn=Compatibility.targetName(t.sourceTable(),sc.name(),mappings);
            ColumnInfo tc=dstByNorm.get(Compatibility.norm(tn));
            if(tc==null) throw new SQLException("Mapping Oracle absent: "+sc.name());
            targetCols.add(tc);
        }

        BigDecimal max=maxId(sybase,t.sourceSchema(),t.sourceTable(),id);
        BigDecimal last=state.load(t.sourceTable());
        if(max==null){ LOG.info("Table vide: "+t.sourceTable()); return; }

        ErrorLogger errors=new ErrorLogger(config.errorDir(),t.sourceTable());
        BigDecimal range=new BigDecimal(config.rangeSize());
        LOG.info("MAX("+id+")="+max+" ; reprise depuis "+last+" ; range="+range);

        while(last.compareTo(max)<0){
            BigDecimal upper=last.add(range);
            if(upper.compareTo(max)>0) upper=max;
            migrateRange(sybase,oracle,t,s,targetCols,id,last,upper,errors);
            oracle.commit();
            state.save(t.sourceTable(),upper);
            LOG.info("Plage validée "+last+" -> "+upper);
            last=upper;
        }

        if(config.syncIdentity()) syncIdentity(oracle,t,targetCols.get(0));
        LOG.info("MIGRATION TERMINEE: "+t.sourceTable());
    }

    private void migrateRange(Connection sybase,Connection oracle,TableConfig t,List<ColumnInfo> s,
                               List<ColumnInfo> targetCols,String id,BigDecimal low,BigDecimal high,
                               ErrorLogger errors)throws Exception {
        String select=buildSelect(t,s,id);
        String insert=buildInsert(t,targetCols);
        LOG.info("SELECT plage "+low+" -> "+high);

        try(PreparedStatement ps=sybase.prepareStatement(select,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
            PreparedStatement ins=oracle.prepareStatement(insert)) {
            ps.setBigDecimal(1,low);
            ps.setBigDecimal(2,high);
            ps.setFetchSize(config.fetchSize());
            if(config.queryTimeout()>0) ps.setQueryTimeout(config.queryTimeout());

            int n=0, inBatch=0;
            try(ResultSet rs=ps.executeQuery()){
                while(rs.next()){
                    n++; inBatch++;
                    for(int i=0;i<s.size();i++)
                        ValueBinder.bind(ins,i+1,rs,s.get(i).jdbcType(),targetCols.get(i).jdbcType());
                    ins.addBatch();
                    if(inBatch>=config.batchSize()){
                        try { ins.executeBatch(); }
                        catch(SQLException e){ oracle.rollback(); errors.log(t.sourceTable(),e.getMessage()); throw e; }
                        inBatch=0;
                    }
                }
                if(inBatch>0){
                    try { ins.executeBatch(); }
                    catch(SQLException e){ oracle.rollback(); errors.log(t.sourceTable(),e.getMessage()); throw e; }
                }
            }
            LOG.info("Lignes lues dans la plage: "+n);
        }
    }

    private String buildSelect(TableConfig t,List<ColumnInfo> s,String id){
        String cols=s.stream().map(c->Db.q(c.name())).reduce((a,b)->a+","+b).orElseThrow();
        return "SELECT "+cols+" FROM "+Db.table(t.sourceSchema(),t.sourceTable())+
                " WHERE "+Db.q(id)+" > ? AND "+Db.q(id)+" <= ? ORDER BY "+Db.q(id);
    }
    private String buildInsert(TableConfig t,List<ColumnInfo> d){
        String cols=d.stream().map(c->Db.q(c.name())).reduce((a,b)->a+","+b).orElseThrow();
        String vals=String.join(",",Collections.nCopies(d.size(),"?"));
        return "INSERT INTO "+Db.table(t.targetSchema(),t.targetTable())+" ("+cols+") VALUES ("+vals+")";
    }
    private BigDecimal maxId(Connection c,String schema,String table,String id)throws SQLException{
        String sql="SELECT MAX("+Db.q(id)+") FROM "+Db.table(schema,table);
        try(PreparedStatement ps=c.prepareStatement(sql); ResultSet rs=ps.executeQuery()){
            if(!rs.next()) return null;
            return rs.getBigDecimal(1);
        }
    }
    private void syncIdentity(Connection c,TableConfig t,ColumnInfo id)throws SQLException{
        String sql="ALTER TABLE "+Db.table(t.targetSchema(),t.targetTable())+
                " MODIFY "+Db.q(id.name())+" GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH LIMIT VALUE)";
        try(Statement st=c.createStatement()){ st.execute(sql); c.commit(); }
        LOG.info("Identity Oracle synchronisée: "+id.name());
    }
}