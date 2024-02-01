package com.github.cluelessskywatcher.chrysocyon.chrysql;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

import com.github.cluelessskywatcher.chrysocyon.chrysql.exceptions.BadSyntaxException;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;

public class ChrySQLLexer {
    public static final Collection<String> KEYWORDS = Arrays.asList(
        "select", "create", "insert", "from", "where", "and",
        "into", "values", "delete", "update", "set", "table", "view", "index", "as", "on"
    );

    public static final Collection<String> DATATYPES = Arrays.asList(
        "int", "varstr", "long"  
    );

    private StreamTokenizer tokenizer;

    public ChrySQLLexer(String s) {
        this.tokenizer = new StreamTokenizer(new StringReader(s));
        this.tokenizer.ordinaryChar('.');
        this.tokenizer.wordChars('_', '_');
        this.tokenizer.lowerCaseMode(true);
        nextToken();
    }

    private void nextToken() {
        try {
            tokenizer.nextToken();
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new BadSyntaxException("Bad Syntax: Failed to read token");
        }
    }

    public boolean matchDelimiter(char d) {
        return d == (char) tokenizer.ttype;
    }

    public boolean matchDataType(TupleDataType dataType) {
        return tokenizer.ttype == StreamTokenizer.TT_WORD &&
            DATATYPES.contains(tokenizer.sval) &&
            tokenizer.sval.equals(dataType.toString());
    }
    
    public boolean matchInteger() {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER;
    }

    public boolean matchString() {
        return (char) tokenizer.ttype == '\"';
    }

    public boolean matchKeyword(String w) {
        return tokenizer.ttype == StreamTokenizer.TT_WORD &&
            tokenizer.sval.equals(w);
    }

    public boolean matchIdentifier() {
        return tokenizer.ttype == StreamTokenizer.TT_WORD &&
            !KEYWORDS.contains(tokenizer.sval) && 
            !DATATYPES.contains(tokenizer.sval);
    }

    public void consumeDelimiter(char d) {
        if (!matchDelimiter(d)) {
            throw new BadSyntaxException("Bad syntax: Delimiter mismatch");
        }
        nextToken();
    }

    public void consumeDataType(TupleDataType dataType) {
        if (!matchDataType(dataType)) {
            throw new BadSyntaxException("Bad syntax: Expected Data Type not found");
        }
        nextToken();
    }

    public int consumeInteger() {
        if (!matchInteger()) {
            throw new BadSyntaxException("Bad syntax: Integer mismatch");
        }
        int i = (int) tokenizer.nval;
        nextToken();
        return i;
    }

    public String consumeString() {
        if (!matchString()) {
            throw new BadSyntaxException("Bad syntax: String mismatch");
        }
        String s = tokenizer.sval;
        nextToken();
        return s;
    }

    public void consumeKeyword(String w) {
        if (!matchKeyword(w)) {
            throw new BadSyntaxException("Bad syntax: Keyword mismatch");
        }
        nextToken();
    }

    public String consumeIdentifier() {
        if (!matchIdentifier()) {
            throw new BadSyntaxException("Bad syntax: Identifier mismatch");
        }
        String id = tokenizer.sval;
        nextToken();
        return id;
    }
}
