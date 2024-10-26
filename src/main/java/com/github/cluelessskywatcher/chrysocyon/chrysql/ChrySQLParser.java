package com.github.cluelessskywatcher.chrysocyon.chrysql;

import java.util.ArrayList;
import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewIndexStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewViewStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.DeleteFromTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertIntoTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.UpdateTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectFromTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.exceptions.BadSyntaxException;
import com.github.cluelessskywatcher.chrysocyon.chrysql.exceptions.ParsingException;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.ExpressionOperator;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.PredicateExpression;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.PredicateTerm;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.QueryPredicate;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class ChrySQLParser {
    private ChrySQLLexer lexer;

    public ChrySQLParser(String query) {
        lexer = new ChrySQLLexer(query);
    }

    public String field() {
        return lexer.consumeIdentifier();
    }

    public DataField constant() {
        if (lexer.matchInteger()) {
            return new IntegerField(lexer.consumeInteger());
        }
        else if (lexer.matchString()) {
            return new VarStringField(lexer.consumeString(), 100);
        }
        throw new ParsingException("Cannot parse constant");
    }

    public PredicateExpression expression() {
        if (lexer.matchIdentifier()) {
            return new PredicateExpression(field());
        }
        else {
            return new PredicateExpression(constant());
        }
    }

    public PredicateTerm term() {
        PredicateExpression lhs = expression();
        ExpressionOperator op;
        if (lexer.matchDelimiter('>')) {
            op = ExpressionOperator.GT;
            lexer.consumeDelimiter('>');
        } else if (lexer.matchDelimiter('<')) {
            op = ExpressionOperator.LT;
            lexer.consumeDelimiter('<');
        } else if (lexer.matchDelimiter('!')) {
            lexer.consumeDelimiter('!');
            if (lexer.matchDelimiter('=')) {
                op = ExpressionOperator.NOT_EQUALS;
                lexer.consumeDelimiter('=');
            } else {
                throw new ParsingException("Bad symbol detected");
            }
        } else {
            lexer.consumeDelimiter('=');
            op = ExpressionOperator.EQUALS;
        }
        PredicateExpression rhs = expression();
        return new PredicateTerm(lhs, rhs, op);
    }

    public QueryPredicate predicate() {
        QueryPredicate predicate = new QueryPredicate(term());
        if (lexer.matchKeyword("and")) {
            lexer.consumeKeyword("and");
            predicate.conjoin(new QueryPredicate(term()));
        }

        return predicate;
    }

    public List<String> commaFieldListOrAsterisk(boolean findAsterisk) {
        List<String> l = new ArrayList<>();
        if (findAsterisk) {
            if (lexer.matchDelimiter('*')) {
                lexer.consumeDelimiter('*');
                return l;
            }
        }

        l.add(field());
        if (lexer.matchDelimiter(',')) {
            lexer.consumeDelimiter(',');
            l.addAll(commaFieldListOrAsterisk(false));
        }
        return l;
    }

    public List<String> commaIdList() {
        List<String> l = new ArrayList<>();
        l.add(lexer.consumeIdentifier());
        if (lexer.matchDelimiter(',')) {
            lexer.consumeDelimiter(',');
            l.addAll(commaIdList());
        }
        return l;
    }

    public ChrySQLStatement parse() {
        if (lexer.matchKeyword("select")) {
            return parseSelect();
        }
        return parseModification();
    }

    public ChrySQLStatement parseSelect() {
        lexer.consumeKeyword("select");
        List<String> selectList = commaFieldListOrAsterisk(true);
        lexer.consumeKeyword("from");
        List<String> tableNames = commaIdList();
        QueryPredicate predicate = new QueryPredicate();
        if (lexer.matchKeyword("where")) {
            lexer.consumeKeyword("where");
            predicate = predicate();
        }
        try {
            lexer.consumeDelimiter(';');
        }
        catch (BadSyntaxException e) {
            throw new ParsingException("Missing query-ending semicolon");
        }
        return new SelectFromTableStatement(tableNames, selectList, predicate);
    }

    public ChrySQLStatement parseModification() {
        if (lexer.matchKeyword("insert")) {
            return parseInsert();
        }
        else if (lexer.matchKeyword("create")) {
            return parseCreate();
        }
        else if (lexer.matchKeyword("update")) {
            return parseUpdate();
        }
        else if (lexer.matchKeyword("delete")) {
            return parseDelete();
        }
        return null;
    }

    private ChrySQLStatement parseUpdate() {
        lexer.consumeKeyword("update");
        String tableName = lexer.consumeIdentifier();
        lexer.consumeKeyword("set");
        String fieldName = lexer.consumeIdentifier();
        lexer.consumeDelimiter('=');
        PredicateExpression expression = expression();
        lexer.consumeKeyword("where");
        QueryPredicate predicate = predicate();
        lexer.consumeDelimiter(';');
        return new UpdateTableStatement(tableName, fieldName, expression, predicate);
    }

    private DeleteFromTableStatement parseDelete() {
        lexer.consumeKeyword("delete");
        lexer.consumeKeyword("from");
        String tableName = lexer.consumeIdentifier();
        QueryPredicate predicate = new QueryPredicate();
        if (lexer.matchKeyword("where")) {
            lexer.consumeKeyword("where");
            predicate = predicate();
        }

        try {
            lexer.consumeDelimiter(';');
        } catch (BadSyntaxException e) {
            throw new ParsingException("Missing query-ending semicolon");
        }

        return new DeleteFromTableStatement(tableName, predicate);
    }

    private ChrySQLStatement parseCreate() {
        lexer.consumeKeyword("create");
        if (lexer.matchKeyword("table")) {
            return parseCreateTable();
        }
        if (lexer.matchKeyword("view")) {
            return parseCreateView();
        }
        else
            return parseCreateIndex();
    }

    private ChrySQLStatement parseCreateIndex() {
        lexer.consumeKeyword("index");
        String indexName = lexer.consumeIdentifier();
        lexer.consumeKeyword("on");
        String tableName = lexer.consumeIdentifier();
        lexer.consumeDelimiter('(');
        String fieldName = lexer.consumeIdentifier();
        lexer.consumeDelimiter(')');
        return new CreateNewIndexStatement(indexName, tableName, fieldName);
    }

    private ChrySQLStatement parseCreateView() {
        lexer.consumeKeyword("view");
        String viewName = lexer.consumeIdentifier();
        lexer.consumeKeyword("as");
        ChrySQLStatement query = parseSelect();
        return new CreateNewViewStatement(viewName, query);
    }

    private ChrySQLStatement parseCreateTable() {
        lexer.consumeKeyword("table");
        String tableName = lexer.consumeIdentifier();
        lexer.consumeDelimiter('(');
        TupleSchema schema = fieldDefinitions();
        lexer.consumeDelimiter(')');
        lexer.consumeDelimiter(';');

        return new CreateNewTableStatement(tableName, schema);
    }

    private TupleSchema fieldDefinitions() {
        TupleSchema schema = fieldDefinition();
        if (lexer.matchDelimiter(',')) {
            lexer.consumeDelimiter(',');
            schema.addAll(fieldDefinitions());
        }
        return schema;
    }

    private TupleSchema fieldDefinition() {
        TupleSchema schema = new TupleSchema();

        if (lexer.matchDataType(TupleDataType.INTEGER)) {
            lexer.consumeDataType(TupleDataType.INTEGER);
            String fieldName = lexer.consumeIdentifier();
            schema.addField(new IntegerInfo(), fieldName);
        }
        else if (lexer.matchDataType(TupleDataType.VARSTR)) {
            lexer.consumeDataType(TupleDataType.VARSTR);
            try {
                lexer.consumeDelimiter('(');
                int fieldLength = lexer.consumeInteger();
                lexer.consumeDelimiter(')');
                String fieldName = lexer.consumeIdentifier();
                schema.addField(new VarStringInfo(fieldLength), fieldName);
            }
            catch (BadSyntaxException e) {
                throw new ParsingException("Need to specify varstr length");
            }
        }
        else {
            throw new ParsingException("Need a valid data-type");
        }

        return schema;
    }

    private List<DataField> valueList() {
        List<DataField> l = new ArrayList<>();
        l.add(constant());
        if (lexer.matchDelimiter(',')) {
            lexer.consumeDelimiter(',');
            l.addAll(valueList());
        }

        return l;
    }

    private ChrySQLStatement parseInsert() {
        lexer.consumeKeyword("insert");
        lexer.consumeKeyword("into");
        String tableName = field();
        lexer.consumeDelimiter('(');
        List<String> fieldList = commaIdList();
        lexer.consumeDelimiter(')');
        lexer.consumeKeyword("values");
        lexer.consumeDelimiter('(');
        List<DataField> values = valueList();
        lexer.consumeDelimiter(')');
        lexer.consumeDelimiter(';');

        return new InsertIntoTableStatement(tableName, fieldList, values);
    }
}
