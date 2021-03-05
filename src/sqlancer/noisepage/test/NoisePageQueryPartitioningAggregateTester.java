package sqlancer.noisepage.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewAliasNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema.NoisePageCompositeDataType;
import sqlancer.noisepage.NoisePageSchema.NoisePageDataType;
import sqlancer.noisepage.NoisePageToStringVisitor;
import sqlancer.noisepage.ast.NoisePageExpression;
import sqlancer.noisepage.ast.NoisePageSelect;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator.NoisePageAggregateFunction;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator.NoisePageBinaryArithmeticOperator;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator.NoisePageCastOperation;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator.NoisePageUnaryPostfixOperator;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator.NoisePageUnaryPrefixOperator;

public class NoisePageQueryPartitioningAggregateTester extends NoisePageQueryPartitioningBase implements TestOracle {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public NoisePageQueryPartitioningAggregateTester(NoisePageGlobalState state) {
        super(state);
        NoisePageErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        NoisePageAggregateFunction aggregateFunction = Randomly.fromOptions(NoisePageAggregateFunction.MAX,
                NoisePageAggregateFunction.MIN, NoisePageAggregateFunction.SUM, NoisePageAggregateFunction.COUNT,
                NoisePageAggregateFunction.AVG/* , NoisePageAggregateFunction.STDDEV_POP */);
        NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction> aggregate = gen
                .generateArgsForAggregate(aggregateFunction);
        List<Node<NoisePageExpression>> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        originalQuery = NoisePageToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
                "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
//        state.getState().queryString = "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult
//                + "\n-- " + secondResult;
        if (firstResult == null && secondResult != null
                || firstResult != null && (!firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult))) {
            if (secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(NoisePageSelect select,
            NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction> aggregate, List<Node<NoisePageExpression>> from) {
        String metamorphicQuery;
        Node<NoisePageExpression> whereClause = gen.generateExpression();
        Node<NoisePageExpression> negatedClause = new NewUnaryPrefixOperatorNode<>(whereClause,
                NoisePageUnaryPrefixOperator.NOT);
        Node<NoisePageExpression> notNullClause = new NewUnaryPostfixOperatorNode<>(whereClause,
                NoisePageUnaryPostfixOperator.IS_NULL);
        List<Node<NoisePageExpression>> mappedAggregate = mapped(aggregate);
        NoisePageSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        NoisePageSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        NoisePageSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate).toString() + " FROM (";
        metamorphicQuery += NoisePageToStringVisitor.asString(leftSelect) + " UNION ALL "
                + NoisePageToStringVisitor.asString(middleSelect) + " UNION ALL "
                + NoisePageToStringVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getString(1);
            }
            return resultString;
        } catch (SQLException e) {
            if (!e.getMessage().contains("Not implemented type")) {
                throw new AssertionError(queryString, e);
            } else {
                throw new IgnoreMeException();
            }
        }
    }

    private List<Node<NoisePageExpression>> mapped(NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction> aggregate) {
        NoisePageCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(Arrays.asList(aggregate));
        case AVG:
            NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction> sum = new NewFunctionNode<>(aggregate.getArgs(),
                    NoisePageAggregateFunction.SUM);
            count = new NoisePageCastOperation(new NewFunctionNode<>(aggregate.getArgs(), NoisePageAggregateFunction.COUNT),
                    new NoisePageCompositeDataType(NoisePageDataType.FLOAT, 8));
            return aliasArgs(Arrays.asList(sum, count));
//        case STDDEV_POP:
//            NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction> sumSquared = new NewFunctionNode<>(
//                    Arrays.asList(new NewBinaryOperatorNode<>(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
//                            NoisePageBinaryArithmeticOperator.MULT)),
//                    NoisePageAggregateFunction.SUM);
//            count = new NoisePageCastOperation(
//                    new NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction>(aggregate.getArgs(),
//                            NoisePageAggregateFunction.COUNT),
//                    new NoisePageCompositeDataType(NoisePageDataType.FLOAT, 8));
//            NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction> avg = new NewFunctionNode<>(aggregate.getArgs(),
//                    NoisePageAggregateFunction.AVG);
//            return aliasArgs(Arrays.asList(sumSquared, count, avg));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<Node<NoisePageExpression>> aliasArgs(List<Node<NoisePageExpression>> originalAggregateArgs) {
        List<Node<NoisePageExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<NoisePageExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<NoisePageExpression>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
//        case STDDEV_POP:
//            return "sqrt(SUM(agg0)/SUM(agg1)-SUM(agg2)*SUM(agg2))";
        case AVG:
            return "SUM(agg0::FLOAT)/SUM(agg1)::FLOAT";
        case COUNT:
            return NoisePageAggregateFunction.SUM.toString() + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private NoisePageSelect getSelect(List<Node<NoisePageExpression>> aggregates, List<Node<NoisePageExpression>> from,
            Node<NoisePageExpression> whereClause, List<Node<NoisePageExpression>> joinList) {
        NoisePageSelect leftSelect = new NoisePageSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
