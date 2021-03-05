package sqlancer.noisepage.gen;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.Randomly;
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
            System.out.println("update generator: "+columns.get(i).getName());
            System.out.println("update generator: "+columns.get(i).getType());
//            if (Randomly.getBooleanWithSmallProbability()) {
//                System.out.println("small pos");
//                expr = gen.generateExpression();
//                NoisePageErrors.addExpressionErrors(errors);
//            } else {
//                System.out.println("big pos");
////                expr = gen.generateConstant();
//                expr = gen.generateConstant(columns.get(i).getType());
//            }
            expr = gen.generateConstant(columns.get(i).getType());
            System.out.println(expr.toString());
            sb.append(NoisePageToStringVisitor.asString(expr));
        }
        NoisePageErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
