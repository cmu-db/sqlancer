package sqlancer.noisepage.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class NoisePageConstant implements Node<NoisePageExpression> {

    private NoisePageConstant() {
    }

    public static class NoisePageNullConstant extends NoisePageConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class NoisePageIntConstant extends NoisePageConstant {

        private final long value;

        public NoisePageIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

    }

    public static class NoisePageDoubleConstant extends NoisePageConstant {

        private final double value;

        public NoisePageDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }

    }

    public static class NoisePageTextConstant extends NoisePageConstant {

        private final String value;

        public NoisePageTextConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''") + "'";
        }

    }

    public static class NoisePageBitConstant extends NoisePageConstant {

        private final String value;

        public NoisePageBitConstant(long value) {
            this.value = Long.toBinaryString(value);
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "B'" + value + "'";
        }

    }

    public static class NoisePageDateConstant extends NoisePageConstant {

        public String textRepr;

        public NoisePageDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }

    }

    public static class NoisePageTimestampConstant extends NoisePageConstant {

        public String textRepr;

        public NoisePageTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }

    }

    public static class NoisePageBooleanConstant extends NoisePageConstant {

        private final boolean value;

        public NoisePageBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static Node<NoisePageExpression> createStringConstant(String text) {
        return new NoisePageTextConstant(text);
    }

    public static Node<NoisePageExpression> createFloatConstant(double val) {
        return new NoisePageDoubleConstant(val);
    }

    public static Node<NoisePageExpression> createIntConstant(long val) {
        return new NoisePageIntConstant(val);
    }

    public static Node<NoisePageExpression> createNullConstant() {
        return new NoisePageNullConstant();
    }

    public static Node<NoisePageExpression> createBooleanConstant(boolean val) {
        return new NoisePageBooleanConstant(val);
    }

    public static Node<NoisePageExpression> createDateConstant(long integer) {
        return new NoisePageDateConstant(integer);
    }

    public static Node<NoisePageExpression> createTimestampConstant(long integer) {
        return new NoisePageTimestampConstant(integer);
    }

}
