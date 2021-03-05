package sqlancer.noisepage.gen;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.NoisePageToStringVisitor;
import sqlancer.noisepage.ast.NoisePageExpression;

public final class NoisePageIndexGenerator {

    private static int index_num = 0;
    private NoisePageIndexGenerator() {
    }

    public static SQLQueryAdapter getQuery(NoisePageGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
//        if (Randomly.getBoolean()) {
//            errors.add("Cant create unique index, table contains duplicate data on indexed column(s)");
//            sb.append("UNIQUE ");
//        }
        sb.append("INDEX ");
        NoisePageTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
//        String indexName = getNewIndexName(table);
        String indexName = "i" + index_num;
        index_num += 1;
        sb.append((indexName));
        System.out.println("sb index name: "+indexName);
//        sb.append(Randomly.fromOptions("i0", "i1", "i2", "i3", "i4")); // cannot query this information
        sb.append(" ON ");
        sb.append(table.getName());
        sb.append("(");
        List<NoisePageColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
//        if (Randomly.getBoolean()) {
//            sb.append(" WHERE ");
//            Node<NoisePageExpression> expr = new NoisePageExpressionGenerator(globalState).setColumns(table.getColumns())
//                    .generateExpression();
//            sb.append(NoisePageToStringVisitor.asString(expr));
//        }
        errors.add("already exists!");
        if (globalState.getDmbsSpecificOptions().testRowid) {
            errors.add("Cannot create an index on the rowid!");
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

//    private static String getNewIndexName(NoisePageTable randomTable) {
//        List<TableIndex> indexes = randomTable.getIndexes();
//        int indexI = 0;
//        while (true) {
//            String indexName = SQLite3Common.createIndexName(indexI++);
//            if (indexes.stream().noneMatch(i -> i.getIndexName().equals(indexName))) {
//                return indexName;
//            }
//        }
//    }
}
