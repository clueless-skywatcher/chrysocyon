package com.github.cluelessskywatcher.chrysocyon.chrysql;

import java.util.ArrayList;
import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.exceptions.BadSyntaxException;
import com.github.cluelessskywatcher.chrysocyon.chrysql.exceptions.ParsingException;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.PredExpression;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.PredTerm;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.QueryPredicate;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;

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

    public PredExpression expression() {
        if (lexer.matchIdentifier()) {
            return new PredExpression(field());
        }
        else {
            return new PredExpression(constant());
        }
    }

    public PredTerm term() {
        PredExpression lhs = expression();
        lexer.consumeDelimiter('=');
        PredExpression rhs = expression();
        return new PredTerm(lhs, rhs);
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
        return new SelectTableStatement(tableNames, selectList, predicate);
    }

    public ChrySQLStatement parseModification() {
        if (lexer.matchKeyword("insert")) {
            return parseInsert();
        }
        else if (lexer.matchKeyword("create")) {
            return parseCreate();
        }
        else if (lexer.matchKeyword("update")) {
            return parseCreate();
        }
        else
            return parseDelete();
    }

    private ChrySQLStatement parseDelete() {
        throw new UnsupportedOperationException("Unimplemented method 'parseDelete'");
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
        throw new UnsupportedOperationException("Unimplemented method 'parseCreateIndex'");
    }

    private ChrySQLStatement parseCreateView() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parseCreateView'");
    }

    private ChrySQLStatement parseCreateTable() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parseCreateTable'");
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

        return new InsertTableStatement(tableName, fieldList, values);
    }
}
