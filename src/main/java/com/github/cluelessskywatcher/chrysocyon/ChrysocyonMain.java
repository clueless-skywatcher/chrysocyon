package com.github.cluelessskywatcher.chrysocyon;

import java.util.Scanner;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLParser;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.exceptions.ParsingException;
import com.github.cluelessskywatcher.chrysocyon.metadata.exceptions.TableDoesNotExistException;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.exceptions.IncomparableDataFieldException;

public class ChrysocyonMain {
    public static void main(String[] args) {
        Chrysocyon db = Chrysocyon.getInstance();
        ChrysoTransaction tx = db.newTransaction();

        Scanner scanner = new Scanner(System.in);
        System.out.print(">> ");
        while (scanner.hasNextLine()) {
            String query = scanner.nextLine();
            // Quit, committing the current transaction
            if (query.equals("!q")) {
                tx.commit();
                break;
            } else {
                try {
                    ChrySQLParser parser = new ChrySQLParser(query);
                    ChrySQLStatement stmt = parser.parse();
                    if (stmt == null) {
                        continue;
                    }
                    stmt.execute(db, tx);
                    System.out.println(stmt.getResult().toString());
                } catch (ParsingException e) {
                    System.out.println("Bad syntax: " + e.getMessage());
                } catch (TableDoesNotExistException | IncomparableDataFieldException e) {
                    System.out.println(e.getMessage());
                } finally {
                    System.out.print(">> ");
                }
            }
        }

        scanner.close();
    }
}
