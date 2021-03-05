package sqlancer.noisepage.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.ast.newast.NewTernaryNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageSchema.NoisePageCompositeDataType;
import sqlancer.noisepage.NoisePageSchema.NoisePageDataType;
import sqlancer.noisepage.ast.NoisePageConstant;
import sqlancer.noisepage.ast.NoisePageExpression;

import static java.lang.Math.abs;

public final class NoisePageExpressionGenerator extends UntypedExpressionGenerator<Node<NoisePageExpression>, NoisePageColumn> {

    private final NoisePageGlobalState globalState;

    public NoisePageExpressionGenerator(NoisePageGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN, CASE,
        IN, COLLATE, LIKE_ESCAPE
    }

    @Override
    protected Node<NoisePageExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            NoisePageAggregateFunction aggregate = NoisePageAggregateFunction.getRandom();
            allowAggregates = false;
            return new NewFunctionNode<>(generateExpressions(depth + 1, aggregate.getNrArgs()), aggregate);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        if (!globalState.getDmbsSpecificOptions().testCollate) {
            possibleOptions.remove(Expression.COLLATE);
        }
        if (!globalState.getDmbsSpecificOptions().testFunctions) {
            possibleOptions.remove(Expression.FUNC);
        }
        if (!globalState.getDmbsSpecificOptions().testCasts) {
            possibleOptions.remove(Expression.CAST);
        }
        if (!globalState.getDmbsSpecificOptions().testBetween) {
            possibleOptions.remove(Expression.BETWEEN);
        }
        if (!globalState.getDmbsSpecificOptions().testIn) {
            possibleOptions.remove(Expression.IN);
        }
        if (!globalState.getDmbsSpecificOptions().testCase) {
            possibleOptions.remove(Expression.CASE);
        }
        if (!globalState.getDmbsSpecificOptions().testBinaryComparisons) {
            possibleOptions.remove(Expression.BINARY_COMPARISON);
        }
        if (!globalState.getDmbsSpecificOptions().testBinaryLogicals) {
            possibleOptions.remove(Expression.BINARY_LOGICAL);
        }
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case COLLATE:
            return new NewUnaryPostfixOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    NoisePageCollate.getRandom());
        case UNARY_PREFIX:
            return new NewUnaryPrefixOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    NoisePageUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new NewUnaryPostfixOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    NoisePageUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            Operator op = NoisePageBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = NoisePageBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new NewBinaryOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), NoisePageBinaryArithmeticOperator.getRandom());
        case CAST:
            return new NoisePageCastOperation(generateExpression(depth + 1), NoisePageCompositeDataType.getRandom());
        case FUNC:
            DBFunction func = DBFunction.getRandom();
            return new NewFunctionNode<NoisePageExpression, DBFunction>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new NewBetweenOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new NewInOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    generateExpressions(depth + 1, Randomly.smallNumber() + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new NewCaseOperatorNode<NoisePageExpression>(generateExpression(depth + 1),
                    generateExpressions(depth + 1, nr), generateExpressions(depth + 1, nr),
                    generateExpression(depth + 1));
        case LIKE_ESCAPE:
            return new NewTernaryNode<NoisePageExpression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), "LIKE", "ESCAPE");
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<NoisePageExpression> generateColumn() {
        NoisePageColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<NoisePageExpression, NoisePageColumn>(column);
    }

    @Override
    public Node<NoisePageExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
//            System.out.println("Create null constant");
            return NoisePageConstant.createNullConstant();
        }
        NoisePageDataType type = NoisePageDataType.getRandom();
        switch (type) {
        case INT:
            if (!globalState.getDmbsSpecificOptions().testIntConstants) {
                throw new IgnoreMeException();
            }
            return NoisePageConstant.createIntConstant(globalState.getRandomly().getInteger());
        case DATE:
            if (!globalState.getDmbsSpecificOptions().testDateConstants) {
                throw new IgnoreMeException();
            }
            return NoisePageConstant.createDateConstant(globalState.getRandomly().getInteger());
        case TIMESTAMP:
            if (!globalState.getDmbsSpecificOptions().testTimestampConstants) {
                throw new IgnoreMeException();
            }
            return NoisePageConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            if (!globalState.getDmbsSpecificOptions().testStringConstants) {
                throw new IgnoreMeException();
            }
            return NoisePageConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOLEAN:
            if (!globalState.getDmbsSpecificOptions().testBooleanConstants) {
                throw new IgnoreMeException();
            }
            return NoisePageConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            if (!globalState.getDmbsSpecificOptions().testFloatConstants) {
                throw new IgnoreMeException();
            }
            return NoisePageConstant.createFloatConstant(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError();
        }
    }

    public Node<NoisePageExpression> generateConstant(NoisePageCompositeDataType curType) {

        if (Randomly.getBooleanWithSmallProbability()) {
            System.out.println("Create null constant");
            return NoisePageConstant.createNullConstant();
        }

//        NoisePageDataType type = NoisePageDataType.getRandom();
        NoisePageDataType type = curType.getType();
        System.out.println("Expression Generator: "+type.toString()+type.name());
        System.out.println("Expression Generator: "+curType.toString());
        switch (type) {
            case INT:
                System.out.println("Expression Generator: "+curType.toString()+curType.getSize());
                if (!globalState.getDmbsSpecificOptions().testIntConstants) {
                    throw new IgnoreMeException();
                }
                if(curType.getSize()==1){
                    int temp = (int) (abs(globalState.getRandomly().getInteger())%255-128);
                    return NoisePageConstant.createIntConstant(temp);
                }else if (curType.getSize()==2){
                    int temp = (int) (abs(globalState.getRandomly().getInteger())%65536-32768);
                    return NoisePageConstant.createIntConstant(temp);
                }else if(curType.getSize()==4){
                    return NoisePageConstant.createIntConstant(globalState.getRandomly().getInteger());
                }else{
                    return NoisePageConstant.createIntConstant(globalState.getRandomly().getInteger());
                }
//                System.out.println(NoisePageConstant.createIntConstant(globalState.getRandomly().getInteger()));
            case DATE:
                if (!globalState.getDmbsSpecificOptions().testDateConstants) {
                    throw new IgnoreMeException();
                }
                return NoisePageConstant.createDateConstant(globalState.getRandomly().getInteger());
            case TIMESTAMP:
                if (!globalState.getDmbsSpecificOptions().testTimestampConstants) {
                    throw new IgnoreMeException();
                }
                return NoisePageConstant.createTimestampConstant(globalState.getRandomly().getInteger());
            case VARCHAR:
                if (!globalState.getDmbsSpecificOptions().testStringConstants) {
                    throw new IgnoreMeException();
                }
                return NoisePageConstant.createStringConstant(globalState.getRandomly().getString());
            case BOOLEAN:
                if (!globalState.getDmbsSpecificOptions().testBooleanConstants) {
                    throw new IgnoreMeException();
                }
                return NoisePageConstant.createBooleanConstant(Randomly.getBoolean());
            case FLOAT:
                if (!globalState.getDmbsSpecificOptions().testFloatConstants) {
                    throw new IgnoreMeException();
                }
                return NoisePageConstant.createFloatConstant(globalState.getRandomly().getDouble());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public List<Node<NoisePageExpression>> generateOrderBys() {
        List<Node<NoisePageExpression>> expr = super.generateOrderBys();
        List<Node<NoisePageExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<NoisePageExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    public static class NoisePageCastOperation extends NewUnaryPostfixOperatorNode<NoisePageExpression> {

        public NoisePageCastOperation(Node<NoisePageExpression> expr, NoisePageCompositeDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

    public enum NoisePageAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1),
//        STRING_AGG(1),
        FIRST(1), SUM(1);
//        STDDEV_SAMP(1), STDDEV_POP(1), VAR_POP(1),
//        VAR_SAMP(1), COVAR_POP(1), COVAR_SAMP(1);

        private int nrArgs;

        NoisePageAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static NoisePageAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public enum DBFunction {
        // trigonometric functions
        ACOS(1), //
        ASIN(1), //
        ATAN(1), //
        COS(1), //
        SIN(1), //
        TAN(1), //
        COT(1), //
        ATAN2(1), //
        // math functions
        ABS(1), //
        CEIL(1), //
//        CEILING(1), //
        FLOOR(1), //
//        LOG(1), //
        LOG10(1), LOG2(1), //
//        LN(1), //
//        PI(0), //
        SQRT(1), //
        POWER(1), //
        CBRT(1), //
        ROUND(2), //
//        SIGN(1), //
//        DEGREES(1), //
//        RADIANS(1), //
        MOD(2), //
        // string functions
        LENGTH(1), //
        LOWER(1), //
        UPPER(1), //
        SUBSTRING(3), //
        REVERSE(1), //
        CONCAT(1, true), //
//        CONCAT_WS(1, true), CONTAINS(2), //
        PREFIX(2), //
        SUFFIX(2), //
        INSTR(2), //
        PRINTF(1, true), //
//        REGEXP_MATCHES(2), //
//        REGEXP_REPLACE(3), //
        STRIP_ACCENTS(1), //
        // date functions
//        DATE_PART(2), AGE(2),

        COALESCE(3), NULLIF(2),

        // LPAD(3),
        // RPAD(3),
        LTRIM(1), RTRIM(1),
        // LEFT(2), https://github.com/cwida/noisepage/issues/633
        // REPEAT(2),
//        REPLACE(3),
        UNICODE(1),

//        BIT_COUNT(1), BIT_LENGTH(1),
//        LAST_DAY(1),
//        MONTHNAME(1), DAYNAME(1), YEARWEEK(1),
//        DAYOFMONTH(1), WEEKDAY(1),
//        WEEKOFYEAR(1),

        IFNULL(2), IF(3);

        private int nrArgs;
        private boolean isVariadic;

        DBFunction(int nrArgs) {
            this(nrArgs, false);
        }

        DBFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static DBFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            if (isVariadic) {
                return Randomly.smallNumber() + nrArgs;
            } else {
                return nrArgs;
            }
        }

    }

    public enum NoisePageUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        NoisePageUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static NoisePageUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static final class NoisePageCollate implements Operator {

        private final String textRepr;

        private NoisePageCollate(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return "COLLATE " + textRepr;
        }

        public static NoisePageCollate getRandom() {
            return new NoisePageCollate(NoisePageTableGenerator.getRandomCollate());
        }

    }

    public enum NoisePageUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        NoisePageUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static NoisePageUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum NoisePageBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum NoisePageBinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%");
//        AND("&"), OR("|"), XOR("#"), LSHIFT("<<"),
//        RSHIFT(">>");

        private String textRepr;

        NoisePageBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum NoisePageBinaryComparisonOperator implements Operator {

        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE"), SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO"),
        REGEX_POSIX("~"), REGEX_POSIT_NOT("!~");

        private String textRepr;

        NoisePageBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public NewFunctionNode<NoisePageExpression, NoisePageAggregateFunction> generateArgsForAggregate(
            NoisePageAggregateFunction aggregateFunction) {
        return new NewFunctionNode<NoisePageExpression, NoisePageExpressionGenerator.NoisePageAggregateFunction>(
                generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<NoisePageExpression> generateAggregate() {
        NoisePageAggregateFunction aggrFunc = NoisePageAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public Node<NoisePageExpression> negatePredicate(Node<NoisePageExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, NoisePageUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<NoisePageExpression> isNull(Node<NoisePageExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, NoisePageUnaryPostfixOperator.IS_NULL);
    }

}
