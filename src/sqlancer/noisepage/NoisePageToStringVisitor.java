package sqlancer.noisepage;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.noisepage.ast.NoisePageConstant;
import sqlancer.noisepage.ast.NoisePageExpression;
//import sqlancer.noisepage.ast.NoisePageJoin;
import sqlancer.noisepage.ast.NoisePageSelect;

public class NoisePageToStringVisitor extends NewToStringVisitor<NoisePageExpression> {

    @Override
    public void visitSpecific(Node<NoisePageExpression> expr) {
        if (expr instanceof NoisePageConstant) {
            visit((NoisePageConstant) expr);
        } else if (expr instanceof NoisePageSelect) {
            visit((NoisePageSelect) expr);
        }
//        else if (expr instanceof NoisePageJoin) {
//            visit((NoisePageJoin) expr);
//        }
        else {
            throw new AssertionError(expr.getClass());
        }
    }

//    private void visit(NoisePageJoin join) {
//        visit(join.getLeftTable());
//        sb.append(" ");
//        sb.append(join.getJoinType());
//        sb.append(" ");
//        if (join.getOuterType() != null) {
//            sb.append(join.getOuterType());
//        }
//        sb.append(" JOIN ");
//        visit(join.getRightTable());
//        if (join.getOnCondition() != null) {
//            sb.append(" ON ");
//            visit(join.getOnCondition());
//        }
//    }

    private void visit(NoisePageConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(NoisePageSelect select) {
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        }
        visit(select.getFetchColumns());
        sb.append(" FROM ");
        visit(select.getFromList());
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
            sb.append(", ");
        }
        if (!select.getJoinList().isEmpty()) {
            visit(select.getJoinList());
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByExpressions());
        }
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    public static String asString(Node<NoisePageExpression> expr) {
        NoisePageToStringVisitor visitor = new NoisePageToStringVisitor();
        System.out.println("String visitor expr: "+expr.toString());
        visitor.visit(expr);
        System.out.println("String visitor expr: "+visitor.get());
        return visitor.get();
    }

}
