package sqlancer.noisepage.gen;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.NoisePageToStringVisitor;

public class NoisePageInsertGenerator extends AbstractInsertGenerator<NoisePageColumn> {

    private final NoisePageGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public NoisePageInsertGenerator(NoisePageGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(NoisePageGlobalState globalState) throws SQLException {
        return new NoisePageInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() throws SQLException {
        sb.append("INSERT INTO ");
        NoisePageTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<NoisePageColumn> temp = NoisePageSchema.getTableColumns(globalState.getConnection()
                , table.getName());
        List<NoisePageColumn> columns = new ArrayList<>();
        for(NoisePageColumn c: temp){
            double ran = Math.random();
            if (ran<0.5){
                columns.add(c);
            }
        }
        if (columns.size()==0){
            columns.add(temp.get(0));
        }
        sb.append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns);
        NoisePageErrors.addInsertErrors(errors);
        System.out.println("Insert statement: " + sb.toString());
        return new SQLQueryAdapter(sb.toString(), errors);
    }


    @Override
    protected void insertValue(NoisePageColumn tiDBColumn) {
        // TODO: select a more meaningful value
        // disabled default
        sb.append(NoisePageToStringVisitor.asString(new NoisePageExpressionGenerator(globalState).generateConstant(tiDBColumn.getType())));
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            sb.append("DEFAULT");
//        } else {
//            sb.append(NoisePageToStringVisitor.asString(new NoisePageExpressionGenerator(globalState).generateConstant(tiDBColumn.getType())));
//        }
    }

}
