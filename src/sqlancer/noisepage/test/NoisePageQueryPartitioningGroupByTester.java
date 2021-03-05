package sqlancer.noisepage.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageToStringVisitor;
import sqlancer.noisepage.ast.NoisePageExpression;

public class NoisePageQueryPartitioningGroupByTester extends NoisePageQueryPartitioningBase {

    public NoisePageQueryPartitioningGroupByTester(NoisePageGlobalState state) {
        super(state);
        NoisePageErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setGroupByExpressions(select.getFetchColumns());
        select.setWhereClause(null);
        String originalQueryString = NoisePageToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = NoisePageToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = NoisePageToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = NoisePageToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    List<Node<NoisePageExpression>> generateFetchColumns() {
        List<Node<NoisePageExpression>> columns = new ArrayList<>();
        columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ColumnReferenceNode<NoisePageExpression, NoisePageColumn>(c)).collect(Collectors.toList());
        return columns;
    }

}
