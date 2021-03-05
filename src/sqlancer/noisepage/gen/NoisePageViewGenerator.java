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
//import sqlancer.noisepage.NoisePageToStringVisitor;
//
//public final class NoisePageViewGenerator {
//
//    private NoisePageViewGenerator() {
//    }
//
//    public static Query generate(NoisePageGlobalState globalState) {
//        int nrColumns = Randomly.smallNumber() + 1;
//        StringBuilder sb = new StringBuilder("CREATE ");
//        sb.append("VIEW ");
//        sb.append(globalState.getSchema().getFreeViewName());
//        sb.append("(");
//        for (int i = 0; i < nrColumns; i++) {
//            if (i != 0) {
//                sb.append(", ");
//            }
//            sb.append("c");
//            sb.append(i);
//        }
//        sb.append(") AS ");
//        sb.append(NoisePageToStringVisitor.asString(NoisePageRandomQuerySynthesizer.generateSelect(globalState, nrColumns)));
//        Set<String> errors = new HashSet<>();
//        NoisePageErrors.addExpressionErrors(errors);
//        NoisePageErrors.addGroupByErrors(errors);
//        return new QueryAdapter(sb.toString(), errors, true);
//    }
//
//}
