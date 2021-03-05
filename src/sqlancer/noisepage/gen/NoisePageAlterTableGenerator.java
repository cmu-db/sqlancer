//package sqlancer.noisepage.gen;
//
//import java.util.HashSet;
//import java.util.Set;
//
//import sqlancer.Query;
//import sqlancer.QueryAdapter;
//import sqlancer.Randomly;
//import sqlancer.noisepage.NoisePageErrors;
//import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
//import sqlancer.noisepage.NoisePageSchema.NoisePageCompositeDataType;
//import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
//import sqlancer.noisepage.NoisePageToStringVisitor;
//
//public final class NoisePageAlterTableGenerator {
//
//    private NoisePageAlterTableGenerator() {
//    }
//
//    enum Action {
//        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN
//    }
//
//    public static Query getQuery(NoisePageGlobalState globalState) {
//        Set<String> errors = new HashSet<>();
//        errors.add(" does not have a column with name \"rowid\"");
//        errors.add("Table does not contain column rowid referenced in alter statement");
//        StringBuilder sb = new StringBuilder("ALTER TABLE ");
//        NoisePageTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
//        NoisePageExpressionGenerator gen = new NoisePageExpressionGenerator(globalState).setColumns(table.getColumns());
//        sb.append(table.getName());
//        sb.append(" ");
//        Action action = Randomly.fromOptions(Action.values());
//        switch (action) {
//        case ADD_COLUMN:
//            sb.append("ADD COLUMN ");
//            String columnName = table.getFreeColumnName();
//            sb.append(columnName);
//            sb.append(" ");
//            sb.append(NoisePageCompositeDataType.getRandom().toString());
//            break;
//        case ALTER_COLUMN:
//            sb.append("ALTER COLUMN ");
//            sb.append(table.getRandomColumn().getName());
//            sb.append(" SET DATA TYPE ");
//            sb.append(NoisePageCompositeDataType.getRandom().toString());
//            if (Randomly.getBoolean()) {
//                sb.append(" USING ");
//                NoisePageErrors.addExpressionErrors(errors);
//                sb.append(NoisePageToStringVisitor.asString(gen.generateExpression()));
//            }
//            errors.add("Cannot change the type of this column: an index depends on it!");
//            errors.add("Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
//            errors.add("Unimplemented type for cast");
//            errors.add("Conversion:");
//            errors.add("Cannot change the type of a column that has a CHECK constraint specified");
//            break;
//        case DROP_COLUMN:
//            sb.append("DROP COLUMN ");
//            sb.append(table.getRandomColumn().getName());
//            errors.add("named in key does not exist"); // TODO
//            errors.add("Cannot drop this column:");
//            errors.add("Cannot drop column: table only has one column remaining!");
//            errors.add("because there is a CHECK constraint that depends on it");
//            errors.add("because there is a UNIQUE constraint that depends on it");
//            break;
//        default:
//            throw new AssertionError(action);
//        }
//        return new QueryAdapter(sb.toString(), errors, true);
//    }
//
//}
