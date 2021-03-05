package sqlancer.noisepage.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator;

public class NoisePageJoin implements Node<NoisePageExpression> {

    private final TableReferenceNode<NoisePageExpression, NoisePageTable> leftTable;
    private final TableReferenceNode<NoisePageExpression, NoisePageTable> rightTable;
    private final JoinType joinType;
    private final Node<NoisePageExpression> onCondition;
    private OuterType outerType;

    public enum JoinType {
//        NATURAL, RIGHT,
        INNER, LEFT;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum OuterType {
//        FULL, LEFT, RIGHT;
        LEFT;

        public static OuterType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public NoisePageJoin(TableReferenceNode<NoisePageExpression, NoisePageTable> leftTable,
            TableReferenceNode<NoisePageExpression, NoisePageTable> rightTable, JoinType joinType,
            Node<NoisePageExpression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public TableReferenceNode<NoisePageExpression, NoisePageTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<NoisePageExpression, NoisePageTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<NoisePageExpression> getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    public static List<Node<NoisePageExpression>> getJoins(
            List<TableReferenceNode<NoisePageExpression, NoisePageTable>> tableList, NoisePageGlobalState globalState) {
        List<Node<NoisePageExpression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            TableReferenceNode<NoisePageExpression, NoisePageTable> leftTable = tableList.remove(0);
            TableReferenceNode<NoisePageExpression, NoisePageTable> rightTable = tableList.remove(0);
            List<NoisePageColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            NoisePageExpressionGenerator joinGen = new NoisePageExpressionGenerator(globalState).setColumns(columns);
            switch (NoisePageJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(NoisePageJoin.createInnerJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
//            case NATURAL:
//                joinExpressions.add(NoisePageJoin.createNaturalJoin(leftTable, rightTable, OuterType.getRandom()));
//                break;
            case LEFT:
                joinExpressions
                        .add(NoisePageJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
//            case RIGHT:
//                joinExpressions
//                        .add(NoisePageJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
//                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

//    public static NoisePageJoin createRightOuterJoin(TableReferenceNode<NoisePageExpression, NoisePageTable> left,
//            TableReferenceNode<NoisePageExpression, NoisePageTable> right, Node<NoisePageExpression> predicate) {
//        return new NoisePageJoin(left, right, JoinType.RIGHT, predicate);
//    }

    public static NoisePageJoin createLeftOuterJoin(TableReferenceNode<NoisePageExpression, NoisePageTable> left,
            TableReferenceNode<NoisePageExpression, NoisePageTable> right, Node<NoisePageExpression> predicate) {
        return new NoisePageJoin(left, right, JoinType.LEFT, predicate);
    }

    public static NoisePageJoin createInnerJoin(TableReferenceNode<NoisePageExpression, NoisePageTable> left,
            TableReferenceNode<NoisePageExpression, NoisePageTable> right, Node<NoisePageExpression> predicate) {
        return new NoisePageJoin(left, right, JoinType.INNER, predicate);
    }

//    public static Node<NoisePageExpression> createNaturalJoin(TableReferenceNode<NoisePageExpression, NoisePageTable> left,
//            TableReferenceNode<NoisePageExpression, NoisePageTable> right, OuterType naturalJoinType) {
//        NoisePageJoin join = new NoisePageJoin(left, right, JoinType.NATURAL, null);
//        join.setOuterType(naturalJoinType);
//        return join;
//    }

}
