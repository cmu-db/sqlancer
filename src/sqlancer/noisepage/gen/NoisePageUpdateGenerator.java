package sqlancer.noisepage.gen;

import java.util.List;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.NoisePageToStringVisitor;
import sqlancer.noisepage.ast.NoisePageExpression;

public final class NoisePageUpdateGenerator {

    private NoisePageUpdateGenerator() {
    }


    public static SQLQueryAdapter getQuery(NoisePageGlobalState globalState) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        ExpectedErrors errors = new ExpectedErrors();
        NoisePageTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        NoisePageExpressionGenerator gen = new NoisePageExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(" SET ");
        List<NoisePageColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            Node<NoisePageExpression> expr;
            expr = gen.generateConstant(columns.get(i).getType());
            sb.append(NoisePageToStringVisitor.asString(expr));
        }
        NoisePageErrors.addInsertErrors(errors);
        System.out.println("Update statement: " + sb.toString());
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
