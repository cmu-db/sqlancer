package sqlancer.noisepage.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageSchema.NoisePageCompositeDataType;
import sqlancer.noisepage.NoisePageSchema.NoisePageDataType;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.NoisePageSchema.NoisePageTables;
import sqlancer.noisepage.NoisePageToStringVisitor;
import sqlancer.noisepage.ast.NoisePageExpression;
//import sqlancer.noisepage.ast.NoisePageJoin;
import sqlancer.noisepage.ast.NoisePageJoin;
import sqlancer.noisepage.ast.NoisePageSelect;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator.NoisePageCastOperation;

public class NoisePageNoRECOracle extends NoRECBase<NoisePageGlobalState> implements TestOracle {

    private final NoisePageSchema s;

    public NoisePageNoRECOracle(NoisePageGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        NoisePageErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        NoisePageTables randomTables = s.getRandomTableNonEmptyTables();
        List<NoisePageColumn> columns = randomTables.getColumns();
        NoisePageExpressionGenerator gen = new NoisePageExpressionGenerator(state).setColumns(columns);
        Node<NoisePageExpression> randomWhereCondition = gen.generateExpression();
        List<NoisePageTable> tables = randomTables.getTables();
        List<TableReferenceNode<NoisePageExpression, NoisePageTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<NoisePageExpression, NoisePageTable>(t)).collect(Collectors.toList());
//        List<Node<NoisePageExpression>> joins = NoisePageJoin.getJoins(tableList, state);
//        int secondCount = getSecondQuery(tableList.stream().collect(Collectors.toList()), randomWhereCondition, joins);
//        int firstCount = getFirstQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
//                randomWhereCondition, joins);
//        if (firstCount == -1 || secondCount == -1) {
//            throw new IgnoreMeException();
//        }
//        if (firstCount != secondCount) {
//            throw new AssertionError(
//                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
//        }
    }

    private int getSecondQuery(List<Node<NoisePageExpression>> tableList, Node<NoisePageExpression> randomWhereCondition,
            List<Node<NoisePageExpression>> joins) throws SQLException {
        NoisePageSelect select = new NoisePageSelect();
        // select.setGroupByClause(groupBys);
        // NoisePageExpression isTrue = NoisePagePostfixOperation.create(randomWhereCondition,
        // PostfixOperator.IS_TRUE);
        Node<NoisePageExpression> asText = new NewPostfixTextNode<>(new NoisePageCastOperation(
                new NewPostfixTextNode<NoisePageExpression>(randomWhereCondition,
                        " IS NOT NULL AND " + NoisePageToStringVisitor.asString(randomWhereCondition)),
                new NoisePageCompositeDataType(NoisePageDataType.INT, 8)), "as count");
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(tableList);
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + NoisePageToStringVisitor.asString(select) + ") as res";
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getFirstQueryCount(Connection con, List<Node<NoisePageExpression>> tableList, List<NoisePageColumn> columns,
            Node<NoisePageExpression> randomWhereCondition, List<Node<NoisePageExpression>> joins) throws SQLException {
        NoisePageSelect select = new NoisePageSelect();
        // select.setGroupByClause(groupBys);
        // NoisePageAggregate aggr = new NoisePageAggregate(
        List<Node<NoisePageExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<NoisePageExpression, NoisePageColumn>(c)).collect(Collectors.toList());
        // NoisePageAggregateFunction.COUNT);
        // select.setFetchColumns(Arrays.asList(aggr));
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new NoisePageExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = NoisePageToStringVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
