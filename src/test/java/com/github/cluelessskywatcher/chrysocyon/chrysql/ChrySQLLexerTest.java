package com.github.cluelessskywatcher.chrysocyon.chrysql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class ChrySQLLexerTest {
    @Test
    public void testLexer1() {
        String a = "a = b;";
        ChrySQLLexer lexer = new ChrySQLLexer(a);
        
        Assertions.assertTrue(lexer.matchIdentifier());
        Assertions.assertTrue(lexer.consumeIdentifier().equals("a"));
        Assertions.assertTrue(lexer.matchDelimiter('='));
        lexer.consumeDelimiter('=');
        Assertions.assertTrue(lexer.matchIdentifier());
        Assertions.assertTrue(lexer.consumeIdentifier().equals("b"));
        Assertions.assertTrue(lexer.matchDelimiter(';'));
        lexer.consumeDelimiter(';');
    }

    @Test
    public void testLexer2() {

        String a = "select col from table1 where id = 21;";
        ChrySQLLexer lexer = new ChrySQLLexer(a);
        Assertions.assertTrue(lexer.matchKeyword("select"));
        lexer.consumeKeyword("select");
        Assertions.assertTrue(lexer.matchIdentifier());
        Assertions.assertTrue(lexer.consumeIdentifier().equals("col"));
        Assertions.assertTrue(lexer.matchKeyword("from"));
        lexer.consumeKeyword("from");
        Assertions.assertTrue(lexer.matchIdentifier());
        Assertions.assertTrue(lexer.consumeIdentifier().equals("table1"));
        Assertions.assertTrue(lexer.matchKeyword("where"));
        lexer.consumeKeyword("where");
        Assertions.assertTrue(lexer.matchIdentifier());
        Assertions.assertTrue(lexer.consumeIdentifier().equals("id"));
        Assertions.assertTrue(lexer.matchDelimiter('='));
        lexer.consumeDelimiter('=');
        Assertions.assertTrue(lexer.matchInteger());
        Assertions.assertEquals(lexer.consumeInteger(), 21);
    }
}
