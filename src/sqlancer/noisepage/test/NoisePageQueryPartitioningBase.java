package sqlancer.noisepage.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.NoisePageSchema.NoisePageTables;
import sqlancer.noisepage.ast.NoisePageExpression;
//import sqlancer.noisepage.ast.NoisePageJoin;
import sqlancer.noisepage.ast.NoisePageSelect;
import sqlancer.noisepage.gen.NoisePageExpressionGenerator;


public class NoisePageQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<NoisePageExpression>, NoisePageGlobalState> implements TestOracle {

    NoisePageSchema s;
    NoisePageTables targetTables;
    NoisePageExpressionGenerator gen;
    NoisePageSelect select;

    public NoisePageQueryPartitioningBase(NoisePageGlobalState state) {
        super(state);
        NoisePageErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new NoisePageExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new NoisePageSelect();
        select.setFetchColumns(generateFetchColumns());
        List<NoisePageTable> tables = targetTables.getTables();
        List<TableReferenceNode<NoisePageExpression, NoisePageTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<NoisePageExpression, NoisePageTable>(t)).collect(Collectors.toList());
//        List<Node<NoisePageExpression>> joins = NoisePageJoin.getJoins(tableList, state);
//        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<Node<NoisePageExpression>> generateFetchColumns() {
        List<Node<NoisePageExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new NoisePageColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<NoisePageExpression, NoisePageColumn>(c)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<NoisePageExpression>> getGen() {
        return gen;
    }

}
