package sqlancer.noisepage.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.NoisePageSchema.NoisePageTables;
import sqlancer.noisepage.ast.NoisePageConstant;
import sqlancer.noisepage.ast.NoisePageExpression;
//import sqlancer.noisepage.ast.NoisePageJoin;
import sqlancer.noisepage.ast.NoisePageSelect;

public final class NoisePageRandomQuerySynthesizer {

    private NoisePageRandomQuerySynthesizer() {
    }

    public static NoisePageSelect generateSelect(NoisePageGlobalState globalState, int nrColumns) {
        NoisePageTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        NoisePageExpressionGenerator gen = new NoisePageExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        NoisePageSelect select = new NoisePageSelect();
        // TODO: distinct
        // select.setDistinct(Randomly.getBoolean());
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<Node<NoisePageExpression>> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            Node<NoisePageExpression> expression = gen.generateExpression();
            columns.add(expression);
            // } else {
            // columns.add(gen());
            // }
        }
        select.setFetchColumns(columns);
        List<NoisePageTable> tables = targetTables.getTables();
        List<TableReferenceNode<NoisePageExpression, NoisePageTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<NoisePageExpression, NoisePageTable>(t)).collect(Collectors.toList());
//        List<Node<NoisePageExpression>> joins = NoisePageJoin.getJoins(tableList, globalState);
//        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(NoisePageConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(
                    NoisePageConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        return select;
    }

}
