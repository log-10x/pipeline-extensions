package com.log10x.ext.cloud.index.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class QueryFilterEvaluator {
	
	public static final String AND = "&&";
	public static final String OR = "||";
	
    private final Node root;
    private final List<Constant> constants;

    private QueryFilterEvaluator(Node root, List<Constant> constants) {
        this.root = root;
        this.constants = List.copyOf(constants);
    }

    /**
     * Returns a list of all constant-value pairs in the expression tree, each paired with its field, in the order they appear.
     * This method is deterministic; the same expression always returns constants in the same order.
     *
     * @return the list of constants
     */
    public List<Constant> constants() {
        return constants;
    }

    /**
     * Evaluates the expression tree using the provided predicate.
     * For each clause that needs evaluation, the predicate is called with the index of the constant.
     * Supports short-circuiting for && and || operators.
     *
     * @param clausePredicate the function that evaluates clauses by index
     * @return the result of the evaluation
     */
    public boolean evaluate(Function<Integer, Boolean> clausePredicate) {
        return root.evaluate(clausePredicate);
    }

    /**
     * Parses the given expression string into a QueryFilterEvaluator instance.
     * Supports expressions with &&, ||, parentheses, and clauses like field == "value",
     * class.field == "value", includes(field, "value"), includes(class.field, "value").
     *
     * @param expression the expression to parse
     * @return the parsed QueryFilterEvaluator
     * @throws IllegalArgumentException if parsing fails, with user-friendly context
     */
    public static QueryFilterEvaluator parse(String expression) {
        if (expression == null) {
            throw new IllegalArgumentException("Expression cannot be null. Please provide a valid filter expression.");
        }
        return new Parser(expression).parse();
    }

    public static class Constant {
        final String field;
        final String value;

        Constant(String field, String value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Constant constant = (Constant) o;
            return Objects.equals(field, constant.field) && Objects.equals(value, constant.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, value);
        }

        @Override
        public String toString() {
            return field + " == \"" + value + "\"";
        }
    }

    private static abstract class Node {
        protected final String expression;

        Node(String expression) {
            this.expression = expression;
        }

        abstract boolean evaluate(Function<Integer, Boolean> predicate);

        abstract Node withNewExpression(String newExpr);
    }

    private static class AndNode extends Node {
        private final Node left;
        private final Node right;

        AndNode(Node left, Node right, String expression) {
            super(expression);
            this.left = left;
            this.right = right;
        }

        @Override
        boolean evaluate(Function<Integer, Boolean> predicate) {
            if (!left.evaluate(predicate)) {
                return false;
            }
            return right.evaluate(predicate);
        }

        @Override
        public String toString() {
            return "AndNode(" + expression + ")";
        }

        @Override
        Node withNewExpression(String newExpr) {
            return new AndNode(left, right, newExpr);
        }
    }

    private static class OrNode extends Node {
        private final Node left;
        private final Node right;

        OrNode(Node left, Node right, String expression) {
            super(expression);
            this.left = left;
            this.right = right;
        }

        @Override
        boolean evaluate(Function<Integer, Boolean> predicate) {
            if (left.evaluate(predicate)) {
                return true;
            }
            return right.evaluate(predicate);
        }

        @Override
        public String toString() {
            return "OrNode(" + expression + ")";
        }

        @Override
        Node withNewExpression(String newExpr) {
            return new OrNode(left, right, newExpr);
        }
    }

    private static class ClauseNode extends Node {
        private final int index;

        ClauseNode(int index, String expression) {
            super(expression);
            this.index = index;
        }

        @Override
        boolean evaluate(Function<Integer, Boolean> predicate) {
            return predicate.apply(index);
        }

        @Override
        public String toString() {
            return "ClauseNode(" + expression + ", index=" + index + ")";
        }

        @Override
        Node withNewExpression(String newExpr) {
            return new ClauseNode(index, newExpr);
        }
    }

    private static class SpanNode {
        final Node node;
        final int endTokenIdx;

        SpanNode(Node node, int endTokenIdx) {
            this.node = node;
            this.endTokenIdx = endTokenIdx;
        }
    }

    private static class Token {
        final String type;
        final String value;
        final int startPos;
        final int endPos;

        Token(String type, String value, int startPos, int endPos) {
            this.type = type;
            this.value = value;
            this.startPos = startPos;
            this.endPos = endPos;
        }

        @Override
        public String toString() {
            return type + ":" + value;
        }
    }

    private static class Parser {
        private final String input;
        private int pos = 0;
        private final List<Token> tokens = new ArrayList<>();
        private int tokenPos = 0;
        private final List<Constant> constants = new ArrayList<>();

        Parser(String input) {
            this.input = input.trim();
            tokenize();
        }

        QueryFilterEvaluator parse() {
            if (tokens.isEmpty()) {
                throw new IllegalArgumentException("Empty expression provided. Please provide a valid filter expression, e.g., 'field == \"value\"' or 'includes(field, \"value\")'.");
            }
            Node root;
            try {
                SpanNode rootSpan = parseOr(0);
                root = rootSpan.node;
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Failed to parse expression '" + input + "': " + e.getMessage() + ". Parsed tokens so far: " + tokens.subList(0, tokenPos));
            }
            if (tokenPos < tokens.size()) {
                throw new IllegalArgumentException("Unexpected extra tokens after expression in '" + input + "': " + tokens.subList(tokenPos, tokens.size()) + ". Ensure the expression is complete and correctly formatted.");
            }
            return new QueryFilterEvaluator(root, constants);
        }

        private void tokenize() {
            while (pos < input.length()) {
                skipWhitespace();
                if (pos >= input.length()) break;
                char c = input.charAt(pos);
                int startPos = pos;
                if ("().,".indexOf(c) != -1) {
                    pos++;
                    tokens.add(new Token("SYMBOL", String.valueOf(c), startPos, pos));
                } else if (c == '&' && peek(1) == '&') {
                    pos += 2;
                    tokens.add(new Token("OP", AND, startPos, pos));
                } else if (c == '|' && peek(1) == '|') {
                    pos += 2;
                    tokens.add(new Token("OP", OR, startPos, pos));
                } else if (c == '=' && peek(1) == '=') {
                    pos += 2;
                    tokens.add(new Token("OP", "==", startPos, pos));
                } else if (c == '"') {
                    String value = parseString();
                    tokens.add(new Token("STRING", value, startPos, pos));
                } else if (Character.isLetterOrDigit(c) || c == '_') {
                    String value = parseIdentifier();
                    tokens.add(new Token("ID", value, startPos, pos));
                } else {
                    throw new IllegalArgumentException("Invalid character '" + c + "' at position " + pos + " in '" + input + "'. Use only valid characters like letters, digits, underscores, or operators (&&, ||, ==).");
                }
            }
        }

        private void skipWhitespace() {
            while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
                pos++;
            }
        }

        private char peek(int offset) {
            if (pos + offset < input.length()) {
                return input.charAt(pos + offset);
            }
            return '\0';
        }

        private String parseString() {
            int openPos = pos;
            pos++; // skip opening "
            int start = pos;
            while (pos < input.length() && input.charAt(pos) != '"') {
                pos++;
            }
            if (pos >= input.length()) {
                throw new IllegalArgumentException("Unclosed string starting at position " + openPos + " in '" + input + "'. Ensure all string literals are enclosed in double quotes.");
            }
            String value = input.substring(start, pos);
            pos++; // skip closing "
            return value;
        }

        private String parseIdentifier() {
            int start = pos;
            while (pos < input.length() && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_')) {
                pos++;
            }
            return input.substring(start, pos);
        }

        private SpanNode parseOr(int startIdx) {
            SpanNode sn = parseAnd(startIdx);
            Node left = sn.node;
            int currentEnd = sn.endTokenIdx;
            while (matchOp(OR)) {
                SpanNode rightSn = parseAnd(tokenPos);
                String expr = input.substring(tokens.get(startIdx).startPos, tokens.get(rightSn.endTokenIdx).endPos);
                Node orNode = new OrNode(left, rightSn.node, expr);
                left = orNode;
                currentEnd = rightSn.endTokenIdx;
            }
            return new SpanNode(left, currentEnd);
        }

        private SpanNode parseAnd(int startIdx) {
            SpanNode sn = parsePrimary(startIdx);
            Node left = sn.node;
            int currentEnd = sn.endTokenIdx;
            while (matchOp(AND)) {
                SpanNode rightSn = parsePrimary(tokenPos);
                String expr = input.substring(tokens.get(startIdx).startPos, tokens.get(rightSn.endTokenIdx).endPos);
                Node andNode = new AndNode(left, rightSn.node, expr);
                left = andNode;
                currentEnd = rightSn.endTokenIdx;
            }
            return new SpanNode(left, currentEnd);
        }

        private SpanNode parsePrimary(int startIdx) {
            if (matchSymbol("(")) {
                int parenOpenIdx = tokenPos - 1;
                SpanNode innerSn = parseOr(tokenPos);
                expectSymbol(")");
                int parenCloseIdx = tokenPos - 1;
                String fullExpr = input.substring(tokens.get(parenOpenIdx).startPos, tokens.get(parenCloseIdx).endPos);
                Node grouped = innerSn.node.withNewExpression(fullExpr);
                return new SpanNode(grouped, parenCloseIdx);
            } else {
                return parseClause(startIdx);
            }
        }

        private SpanNode parseClause(int startIdx) {
            if (tokenPos < tokens.size() && "ID".equals(tokens.get(tokenPos).type) && "includes".equals(tokens.get(tokenPos).value)) {
                tokenPos++;
                expectSymbol("(");
                String field = parseField();
                expectSymbol(",");
                String constVal = expectString();
                int index = constants.size();
                constants.add(new Constant(field, constVal));
                expectSymbol(")");
                int endIdx = tokenPos - 1;
                String expr = input.substring(tokens.get(startIdx).startPos, tokens.get(endIdx).endPos);
                return new SpanNode(new ClauseNode(index, expr), endIdx);
            } else {
                String field = parseField();
                expectOp("==");
                String constVal = expectString();
                int index = constants.size();
                constants.add(new Constant(field, constVal));
                int endIdx = tokenPos - 1;
                String expr = input.substring(tokens.get(startIdx).startPos, tokens.get(endIdx).endPos);
                return new SpanNode(new ClauseNode(index, expr), endIdx);
            }
        }

        private String parseField() {
            String field = expectId();
            if (tokenPos < tokens.size() && "SYMBOL".equals(tokens.get(tokenPos).type) && ".".equals(tokens.get(tokenPos).value)) {
                tokenPos++;
                field += "." + expectId();
            }
            return field;
        }

        private boolean matchOp(String op) {
            if (tokenPos < tokens.size() && "OP".equals(tokens.get(tokenPos).type) && op.equals(tokens.get(tokenPos).value)) {
                tokenPos++;
                return true;
            }
            return false;
        }

        private boolean matchSymbol(String symbol) {
            if (tokenPos < tokens.size() && "SYMBOL".equals(tokens.get(tokenPos).type) && symbol.equals(tokens.get(tokenPos).value)) {
                tokenPos++;
                return true;
            }
            return false;
        }

        private void expectOp(String op) {
            if (!matchOp(op)) {
                String found = tokenPos < tokens.size() ? tokens.get(tokenPos).toString() : "end of expression";
                throw new IllegalArgumentException("Expected operator '" + op + "' but found '" + found + "' at token position " + tokenPos + " in '" + input + "'. Use '==' for equality or '&&'/'||' for logical operators.");
            }
        }

        private void expectSymbol(String symbol) {
            if (!matchSymbol(symbol)) {
                String found = tokenPos < tokens.size() ? tokens.get(tokenPos).toString() : "end of expression";
                throw new IllegalArgumentException("Expected symbol '" + symbol + "' but found '" + found + "' at token position " + tokenPos + " in '" + input + "'. Ensure proper punctuation, e.g., parentheses or commas.");
            }
        }

        private String expectString() {
            if (tokenPos >= tokens.size() || !"STRING".equals(tokens.get(tokenPos).type)) {
                String found = tokenPos < tokens.size() ? tokens.get(tokenPos).toString() : "end of expression";
                throw new IllegalArgumentException("Expected a string literal in quotes but found '" + found + "' at token position " + tokenPos + " in '" + input + "'. String literals must be enclosed in double quotes, e.g., \"value\".");
            }
            String value = tokens.get(tokenPos).value;
            tokenPos++;
            return value;
        }

        private String expectId() {
            if (tokenPos >= tokens.size() || !"ID".equals(tokens.get(tokenPos).type)) {
                String found = tokenPos < tokens.size() ? tokens.get(tokenPos).toString() : "end of expression";
                throw new IllegalArgumentException("Expected an identifier (e.g., field name) but found '" + found + "' at token position " + tokenPos + " in '" + input + "'. Use letters, digits, or underscores for field names.");
            }
            String value = tokens.get(tokenPos).value;
            tokenPos++;
            return value;
        }
    }

    private static class TestPredicate implements Function<Integer, Boolean> {
        private final Map<Integer, Boolean> values;
        private final List<Integer> called = new ArrayList<>();

        public TestPredicate(Map<Integer, Boolean> values) {
            this.values = values;
        }

        @Override
        public Boolean apply(Integer index) {
            called.add(index);
            if (!values.containsKey(index)) {
                throw new IllegalArgumentException("Unexpected predicate call for index " + index + ".");
            }
            return values.get(index);
        }

        public List<Integer> getCalled() {
            return called;
        }
    }

    private static void test(String expr, List<Constant> expConsts, Object... evalTests) {
        try {
            QueryFilterEvaluator fe = parse(expr);
            List<Constant> consts = fe.constants();
            if (!consts.equals(expConsts)) {
                throw new AssertionError("Constants mismatch for expression \"" + expr + "\": got " + consts + ", expected " + expConsts);
            }
            for (int i = 0; i < evalTests.length; i += 3) {
                @SuppressWarnings("unchecked")
                Map<Integer, Boolean> vals = (Map<Integer, Boolean>) evalTests[i];
                boolean expRes = (Boolean) evalTests[i + 1];
                @SuppressWarnings("unchecked")
                List<Integer> expCalled = (List<Integer>) evalTests[i + 2];
                TestPredicate pred = new TestPredicate(vals);
                boolean res;
                try {
                    res = fe.evaluate(pred);
                } catch (Exception e) {
                    throw new AssertionError("Unexpected exception during evaluation of \"" + expr + "\" with values " + vals + ": " + e.getMessage());
                }
                List<Integer> called = pred.getCalled();
                if (res != expRes || !called.equals(expCalled)) {
                    throw new AssertionError("Evaluation for \"" + expr + "\" with values " + vals + ": got result=" + res + ", called=" + called + "; expected result=" + expRes + ", called=" + expCalled);
                }
            }
        } catch (Exception e) {
            throw new AssertionError("Parse exception for \"" + expr + "\": " + e.getMessage());
        }
    }

    private static void testExpectThrow(String expr, String expMsgPart) {
        try {
            parse(expr);
            throw new AssertionError("Expected parsing to fail for \"" + expr + "\", but no exception was thrown.");
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            if (expMsgPart == null || msg.contains(expMsgPart)) {
                // pass
            } else {
                throw new AssertionError("Wrong exception message for \"" + expr + "\": got \"" + msg + "\", expected message containing \"" + expMsgPart + "\"");
            }
        }
    }

    public static void main(String[] args) {
        // Simple ==
        test("a == \"hello\"", List.of(new Constant("a", "hello")),
                Map.of(0, true), true, List.of(0),
                Map.of(0, false), false, List.of(0)
        );

        // Simple includes
        test("includes(b, \"bye\")", List.of(new Constant("b", "bye")),
                Map.of(0, true), true, List.of(0),
                Map.of(0, false), false, List.of(0)
        );

        // With class
        test("MyClass.field == \"value\"", List.of(new Constant("MyClass.field", "value")),
                Map.of(0, true), true, List.of(0)
        );

        // Includes with class
        test("includes(MyClass.field, \"value with space\")", List.of(new Constant("MyClass.field", "value with space")),
                Map.of(0, true), true, List.of(0)
        );

        // Empty string
        test("field == \"\"", List.of(new Constant("field", "")),
                Map.of(0, true), true, List.of(0)
        );

        // AND with short circuit
        test("(a == \"1\") && (b == \"2\")", List.of(new Constant("a", "1"), new Constant("b", "2")),
                Map.of(0, false), false, List.of(0),
                Map.of(0, true, 1, true), true, List.of(0, 1),
                Map.of(0, true, 1, false), false, List.of(0, 1)
        );

        // OR with short circuit
        test("(a == \"1\") || (b == \"2\")", List.of(new Constant("a", "1"), new Constant("b", "2")),
                Map.of(0, true), true, List.of(0),
                Map.of(0, false, 1, true), true, List.of(0, 1),
                Map.of(0, false, 1, false), false, List.of(0, 1)
        );

        // Precedence: && then ||
        test("a == \"1\" && b == \"2\" || c == \"3\"", List.of(new Constant("a", "1"), new Constant("b", "2"), new Constant("c", "3")),
                Map.of(0, false, 2, true), true, List.of(0, 2),
                Map.of(0, false, 2, false), false, List.of(0, 2),
                Map.of(0, true, 1, true), true, List.of(0, 1),
                Map.of(0, true, 1, false, 2, true), true, List.of(0, 1, 2),
                Map.of(0, true, 1, false, 2, false), false, List.of(0, 1, 2)
        );

        // Parentheses changing precedence
        test("a == \"1\" && (b == \"2\" || c == \"3\")", List.of(new Constant("a", "1"), new Constant("b", "2"), new Constant("c", "3")),
                Map.of(0, false), false, List.of(0),
                Map.of(0, true, 1, true), true, List.of(0, 1),
                Map.of(0, true, 1, false, 2, true), true, List.of(0, 1, 2),
                Map.of(0, true, 1, false, 2, false), false, List.of(0, 1, 2)
        );

        // Nested parentheses
        test("((a == \"1\") && (includes(b, \"2\"))) || (MyClass.c == \"3\")", List.of(new Constant("a", "1"), new Constant("b", "2"), new Constant("MyClass.c", "3")),
                Map.of(0, false), false, List.of(0),
                Map.of(0, true, 1, true), true, List.of(0, 1),
                Map.of(0, false, 2, true), true, List.of(0, 2),
                Map.of(0, true, 1, false, 2, true), true, List.of(0, 1, 2)
        );

        // Complex mixed
        test("(a == \"1\" || b == \"2\") && (c == \"3\" || d == \"4\")", List.of(new Constant("a", "1"), new Constant("b", "2"), new Constant("c", "3"), new Constant("d", "4")),
                Map.of(0, false, 1, false), false, List.of(0, 1),
                Map.of(0, true, 2, true), true, List.of(0, 2),
                Map.of(0, false, 1, true, 2, false, 3, true), true, List.of(0, 1, 2, 3),
                Map.of(0, false, 1, true, 2, false, 3, false), false, List.of(0, 1, 2, 3),
                Map.of(0, true, 2, false, 3, false), false, List.of(0, 2, 3)
        );

        // Deep nesting
        test("(((a == \"1\")))", List.of(new Constant("a", "1")),
                Map.of(0, true), true, List.of(0)
        );

        // Invalid cases
        testExpectThrow("", "Empty expression");
        testExpectThrow("a == hello", "Expected a string literal");
        testExpectThrow("includes(a \"1\")", "Expected symbol ','");
        testExpectThrow("(a == \"1\"", "Expected symbol ')'");
        testExpectThrow("a = \"1\"", "Expected operator '=='");
        testExpectThrow("includes(a, \"1\" extra", "Unexpected extra tokens");
        testExpectThrow(null, "Expression cannot be null");
    }
}
