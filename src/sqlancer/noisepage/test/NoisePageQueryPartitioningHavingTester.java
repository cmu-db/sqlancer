package sqlancer.noisepage.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.TestOracle;
import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageToStringVisitor;
import sqlancer.noisepage.ast.NoisePageExpression;

public class NoisePageQueryPartitioningHavingTester extends NoisePageQueryPartitioningBase implements TestOracle {

    public NoisePageQueryPartitioningHavingTester(NoisePageGlobalState state) {
        super(state);
        NoisePageErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = NoisePageToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(predicate);
        String firstQueryString = NoisePageToStringVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = NoisePageToStringVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = NoisePageToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected Node<NoisePageExpression> generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<Node<NoisePageExpression>> generateFetchColumns() {
        return Arrays.asList(gen.generateHavingClause());
    }

}
