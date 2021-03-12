package sqlancer.noisepage.gen;

import java.sql.SQLException;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.NoisePageToStringVisitor;

import static sqlancer.noisepage.NoisePageSchema.NoisePageDataType.BOOLEAN;

public final class NoisePageDeleteGenerator {

    private NoisePageDeleteGenerator() {

    }

    public static SQLQueryAdapter getQuery(NoisePageGlobalState globalState) throws SQLException {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        NoisePageTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            List<NoisePageSchema.NoisePageColumn> columns = table.getRandomNonEmptyColumnSubset();
            int counter = 0;
            for (int i = 0; i < columns.size(); i++) {
                String value = NoisePageSchema.getRandomColumnValue(globalState.getConnection(),
                        table.getName(), columns.get(i));
                if (counter != 0) {
                    sb.append(" AND ");
                }
                sb.append(columns.get(i).getName());
                sb.append("=");
                if (value == null){
                    sb.append("NULL");
                } else if (value.equals("")) {
                    sb.append("''");
                }else{
                    if (columns.get(i).getType().getType()==BOOLEAN){
                        if (value=="f"){
                            sb.append("false");
                        }else{
                            sb.append("true");
                        }
                    }else {
                        sb.append(value);
                    }
                }
                counter += 1;
            }
            if(counter == 0){
                System.out.append((char) sb.length());
                System.out.append(sb.toString());
                sb.deleteCharAt(sb.length()-1);
            }
        }
        System.out.println("Delete statement: " + sb.toString());
        NoisePageErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
