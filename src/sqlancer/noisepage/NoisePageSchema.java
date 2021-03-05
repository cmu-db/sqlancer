package sqlancer.noisepage;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.postgres.PostgresSchema;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;

public class NoisePageSchema extends AbstractSchema<NoisePageGlobalState, NoisePageTable> {

    public enum NoisePageDataType {

        INT, VARCHAR, BOOLEAN, FLOAT,
        DATE,
        TIMESTAMP;

        public static NoisePageDataType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static class NoisePageCompositeDataType {

        private final NoisePageDataType dataType;

        private final int size;

        public NoisePageCompositeDataType(NoisePageDataType dataType) {
            this.dataType = dataType;
            this.size = -1;
        }

        public NoisePageCompositeDataType(NoisePageDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public NoisePageDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public NoisePageDataType getType(){
            return this.dataType;
        }

        public static NoisePageCompositeDataType getRandom() {
            NoisePageDataType type = NoisePageDataType.getRandom();
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOAT:
                size = 8;
                break;
            case BOOLEAN:
                size = 1;
                break;
            case VARCHAR:
            case DATE:
                size = 4;
                break;
            case TIMESTAMP:
                size = 8;
                break;
            default:
                throw new AssertionError(type);
            }

            return new NoisePageCompositeDataType(type, size);
        }

        public static NoisePageCompositeDataType getInt(int size) {
            return new NoisePageCompositeDataType(NoisePageDataType.INT, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("BIGINT", "INT8");
                case 4:
                    return Randomly.fromOptions("INTEGER", "INT", "INT4");
                case 2:
                    return Randomly.fromOptions("SMALLINT", "INT2");
                case 1:
//                    return Randomly.fromOptions("TINYINT", "INT1");
                    return "TINYINT";
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return "VARCHAR";
            case FLOAT:
                switch (size) {
//                case 8:
//                    return Randomly.fromOptions("DOUBLE", "NUMERIC");
//                case 4:
//                    return Randomly.fromOptions("REAL", "FLOAT4");
                case 8:
                    return Randomly.fromOptions("REAL");
                case 16:
                    return Randomly.fromOptions("DECIMAL");
                default:
                    throw new AssertionError(size);
                }
            case BOOLEAN:
                return Randomly.fromOptions("BOOLEAN", "BOOL");
            case TIMESTAMP:
//                return Randomly.fromOptions("TIMESTAMP", "DATETIME");
                return "TIMESTAMP";
            case DATE:
//                return Randomly.fromOptions("DATE");
                return "DATE";
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class NoisePageColumn extends AbstractTableColumn<NoisePageTable, NoisePageCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;
        private final NoisePageCompositeDataType type;

        public NoisePageColumn(String name, NoisePageCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
            this.type = columnType;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }
        public NoisePageCompositeDataType getType(){
            return this.type;
        }

    }

    public static class NoisePageTables extends AbstractTables<NoisePageTable, NoisePageColumn> {

        public NoisePageTables(List<NoisePageTable> tables) {
            super(tables);
        }

    }

    public NoisePageSchema(List<NoisePageTable> databaseTables) {
        super(databaseTables);
    }

    public NoisePageTables getRandomTableNonEmptyTables() {
        return new NoisePageTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static NoisePageCompositeDataType getColumnType(String typeString) {
        NoisePageDataType primitiveType;
        int size = -1;
        switch (typeString) {
        case "1":
            primitiveType = NoisePageDataType.BOOLEAN;
            break;
        case "2":
            primitiveType = NoisePageDataType.INT;
            size = 1;
            break;
        case "3":
            primitiveType = NoisePageDataType.INT;
            size = 2;
            break;
        case "4":
            primitiveType = NoisePageDataType.INT;
            size = 4;
            break;
        case "5":
            primitiveType = NoisePageDataType.INT;
            size = 8;
            break;
        case "6":
            primitiveType = NoisePageDataType.FLOAT;
            size = 8;
            break;
        case "7":
            primitiveType = NoisePageDataType.FLOAT;
            size = 16;
            break;
        case "8":
            primitiveType = NoisePageDataType.TIMESTAMP;
            size = 8;
            break;
        case "9":
            primitiveType = NoisePageDataType.DATE;
            size = 4;
            break;
        case "10":
            primitiveType = NoisePageDataType.VARCHAR;
            break;
        default:
            System.out.println("assert error typestring");
            throw new AssertionError(typeString);
        }
        return new NoisePageCompositeDataType(primitiveType, size);
    }

    public static class NoisePageTable extends AbstractRelationalTable<NoisePageColumn, TableIndex, NoisePageGlobalState> {

        public NoisePageTable(String tableName, List<NoisePageColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static final class NoisePageIndex extends TableIndex {

        private NoisePageIndex(String indexName) {
            super(indexName);
        }

        public static NoisePageSchema.NoisePageIndex create(String indexName) {
            return new NoisePageSchema.NoisePageIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static NoisePageSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<NoisePageTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        System.out.println("From connection table names: "+tableNames);
        for (String tableName : tableNames) {
            List<NoisePageColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            NoisePageTable t = new NoisePageTable(tableName, databaseColumns, isView);
            for (NoisePageColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);
        }
//        for(NoisePageTable i:databaseTables){
//            System.out.println(i.getName());
//            System.out.println("here");
//        }
        return new NoisePageSchema(databaseTables);
    }

    public static List<String> processResults(ResultSet rs) throws SQLException {
        int numCols = rs.getMetaData().getColumnCount();
        ArrayList<ArrayList<String>> resultRows = new ArrayList<>();
        while (rs.next()) {
            ArrayList<String> resultRow = new ArrayList<>();
            for (int i = 1; i <= numCols; ++i) {
                // TODO(WAN): expose NULL behavior as knob
                if (null == rs.getString(i)) {
                    resultRow.add("");
                } else {
                    resultRow.add(rs.getString(i));
                }
            }
            resultRows.add(resultRow);
        }

        return resultRows.stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public static String getRandomColumnValue(SQLConnection con, String tableName, NoisePageColumn col){
        List<String> values = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            String sql = String.format("SELECT %s FROM %s",col.getName(), tableName);
            s.execute(sql);
            try(ResultSet rs = s.getResultSet()){
                values = processResults(rs);
            }catch (Exception e2){
                System.out.println("Failed to get result set");
            }
        }catch (SQLException e1){
            System.out.println("Create statement failed for getting random value");
        }
        Random rand = new Random();
        System.out.println("Get random column value: "+ values.size());
        System.out.println("Get random column value: "+ col.getType().toString());
        System.out.println("Get random column value: "+ col.getType().getType());
        if(values.size()==0){
            return "";
        }
        if(col.getType().getType()==NoisePageDataType.VARCHAR||col.getType().getType()==NoisePageDataType.TIMESTAMP
        ||col.getType().getType()==NoisePageDataType.DATE){
            return "'"+values.get(rand.nextInt(values.size()))+"'";
        }else{
            return values.get(rand.nextInt(values.size()));
        }
    }

    public static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        List<String> res = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            s.execute("SELECT relname FROM pg_class WHERE relkind = 114;");
            try(ResultSet rs = s.getResultSet()){
                tableNames = processResults(rs);
            }catch (Exception e2){
                System.out.println("Failed to get result set");
            }
        }catch (SQLException e1){
            System.out.println("Create statement failed");
        }
        for(String i:tableNames){
            if(!i.startsWith("pg_")){
                res.add(i);
            }
        }
        return res;
    }

    public static List<NoisePageColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        Set<String> visited = new HashSet<>();
        List<NoisePageColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            String sql = String.format("SELECT attname, atttypid FROM pg_attribute JOIN pg_class ON attrelid = reloid WHERE relname = '%s';",tableName);
//            System.out.println(sql);
            try (ResultSet rs = s.executeQuery(sql)) {
                List<String> colNames = processResults(rs);
//                System.out.println("COLTYPENAME");
//                System.out.println(colNames);
                // get primary key
                for(int i=0;i<colNames.size();i+=2){
//                    boolean isNullable = rs.getString("notnull").contentEquals("false");
//                    boolean isPrimaryKey = rs.getString("pk").contains("true");
//                    NoisePageColumn c = new NoisePageColumn(colNames.get(i), getColumnType(colTypeName), isPrimaryKey, isNullable);
                    // TODO: deal with primary key
                    if(!visited.contains(colNames.get(i))){
                        NoisePageColumn c = new NoisePageColumn(colNames.get(i), getColumnType(colNames.get(i+1)), false, false);
                        columns.add(c);
                        visited.add(colNames.get(i));
                    }
                }
            }catch(Exception e){
                System.out.println("select failed");
            }
        }
//        if (columns.stream().noneMatch(c -> c.isPrimaryKey())) {
//            // https://github.com/cwida/noisepage/issues/589
//            // https://github.com/cwida/noisepage/issues/588
//            // TODO: implement an option to enable/disable rowids
//            columns.add(new NoisePageColumn("rowid", new NoisePageCompositeDataType(NoisePageDataType.INT, 4), false, false));
//        }
//        System.out.println("Get table columns: "+tableName);
//        for(NoisePageColumn i:columns){
//            System.out.println("Get table columns: "+i.getType()+i.getName());
//        }

        return columns;
    }
    public String getFreeTableName(SQLConnection connection) throws SQLException {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("t%d", i++);
            List<String> tableNames = getTableNames(connection);
            if(!tableNames.contains(tableName)){
                return tableName;
            }
        } while (true);
    }

}
