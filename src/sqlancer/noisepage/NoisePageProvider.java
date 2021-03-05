package sqlancer.noisepage;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import sqlancer.AbstractAction;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;

import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.gen.NoisePageDeleteGenerator;
import sqlancer.noisepage.gen.NoisePageIndexGenerator;
import sqlancer.noisepage.gen.NoisePageInsertGenerator;
import sqlancer.noisepage.gen.NoisePageRandomQuerySynthesizer;
import sqlancer.noisepage.gen.NoisePageTableGenerator;
import sqlancer.noisepage.gen.NoisePageUpdateGenerator;
//import sqlancer.noisepage.gen.NoisePageViewGenerator;

public class NoisePageProvider extends SQLProviderAdapter<NoisePageGlobalState, NoisePageOptions> {

    public NoisePageProvider() {
        super(NoisePageGlobalState.class, NoisePageOptions.class);
    }

    public enum Action implements AbstractAction<NoisePageGlobalState> {

        INSERT(NoisePageInsertGenerator::getQuery), //
        CREATE_INDEX(NoisePageIndexGenerator::getQuery), //
//        VACUUM((g) -> new SQLQueryAdapter("VACUUM;")), //
//        ANALYZE((g) -> new SQLQueryAdapter("ANALYZE;")), //
        DELETE(NoisePageDeleteGenerator::getQuery), //
        UPDATE(NoisePageUpdateGenerator::getQuery); //
//        CREATE_VIEW(NoisePageViewGenerator::generate), //
//        EXPLAIN((g) -> {
//            Set<String> errors = new HashSet<>();
//            NoisePageErrors.addExpressionErrors(errors);
//            NoisePageErrors.addGroupByErrors(errors);
//            return new SQLQueryAdapter(
//                    "EXPLAIN " + NoisePageToStringVisitor
//                            .asString(NoisePageRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
//                    errors);
//        });

        private final SQLQueryProvider<NoisePageGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<NoisePageGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(NoisePageGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(NoisePageGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            if (!globalState.getDmbsSpecificOptions().testIndexes) {
                return 0;
            }
            // fall through
        case UPDATE:
            return r.getInteger(0, globalState.getDmbsSpecificOptions().maxNumUpdates + 1);
//        case VACUUM: // seems to be ignored
//        case ANALYZE: // seems to be ignored
//        case EXPLAIN:
//            return r.getInteger(0, 2);
        case DELETE:
            return r.getInteger(0, globalState.getDmbsSpecificOptions().maxNumDeletes + 1);
//        case CREATE_VIEW:
//            return r.getInteger(0, globalState.getDmbsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class NoisePageGlobalState extends SQLGlobalState<NoisePageOptions, NoisePageSchema> {

        @Override
        protected NoisePageSchema readSchema() throws SQLException {
            return NoisePageSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(NoisePageGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success = false;
            do {
//                System.out.println("enter");
                SQLQueryAdapter qt = new NoisePageTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
                if(success){
                    System.out.println(qt.getQueryString()+" query string generate database");
                }
            } while (!success);
        }
//        System.out.println(globalState.getSchema().getDatabaseTables());
        System.out.println("hahaha");
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<NoisePageGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                NoisePageProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        System.out.println("hahaha2");
        se.executeStatements();
    }

//    @Override
//    protected TestOracle getTestOracle(NoisePageGlobalState globalState) throws SQLException {
//        return new CompositeTestOracle(globalState.getDmbsSpecificOptions().oracle.stream().map(o -> {
//            try {
//                return o.create(globalState);
//            } catch (SQLException e1) {
//                throw new AssertionError(e1);
//            }
//        }).collect(Collectors.toList()));
//    }

    @Override
    public SQLConnection createDatabase(NoisePageGlobalState globalState) throws SQLException {
//        String url = "jdbc:postgresql://localhost:15721/";
//        // use host names, url is wrong
//        return DriverManager.getConnection(url, globalState.getOptions().getUserName(),
//                globalState.getOptions().getPassword());
        return makeDefaultConnection();
    }

    public static SQLConnection makeDefaultConnection() throws SQLException {
        return makeConnection("localhost", 15721, "noisepage");
    }

    public static SQLConnection makeConnection(String host, int port, String username) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("prepareThreshold", "0"); // suppress switchover to binary protocol

        // Set prepferQueryMode
        String preferQueryMode = System.getenv("NOISEPAGE_QUERY_MODE");
        if (preferQueryMode == null || preferQueryMode.isEmpty()) {
            // Default as "simple" if NOISEPAGE_QUERY_MODE is not specified
            preferQueryMode = "simple";
        }
        props.setProperty("preferQueryMode", preferQueryMode);

        // Set prepareThreshold if the prepferQueryMode is 'extended'
        if (preferQueryMode.equals("extended")) {
            String prepareThreshold = System.getenv("NOISEPAGE_PREPARE_THRESHOLD");
            if (prepareThreshold != null && !prepareThreshold.isEmpty()) {
                props.setProperty("prepareThreshold", prepareThreshold);
            }
        }

        String url = String.format("jdbc:postgresql://%s:%d/", host, port);
        SQLConnection conn = new SQLConnection(DriverManager.getConnection(url, props));
        return conn;
    }
    @Override
    public String getDBMSName() {
        return "noisepage";
    }

}
