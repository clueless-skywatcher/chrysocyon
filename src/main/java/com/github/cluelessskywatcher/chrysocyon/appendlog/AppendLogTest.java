package com.github.cluelessskywatcher.chrysocyon.appendlog;

import java.util.Iterator;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

public class AppendLogTest {
    private static AppendLogManager lm;

    public static void main(String[] args) {
        Chrysocyon db = new Chrysocyon("logtest", 400, 3);
        lm = db.getLogManager();
        createRecords(1, 35);
        printLogRecords("The log file now has these records:");
        createRecords(36, 70);
        lm.flushToFile(65);
        printLogRecords("The log file now has these records:");
    }

    private static void printLogRecords(String msg) {
        System.out.println(msg);
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] rec = iter.next();
            PageObject p = new PageObject(rec);
            String s = p.getString(0);
            int npos = PageObject.maxStringLength(s.length());
            int val = p.getInt(npos);
            System.out.println("[" + s + ", " + val + "]");
        }
        System.out.println();
    }

    private static void createRecords(int start, int end) {
        System.out.print("Creating records: ");
        for (int i = start; i <= end; i++) {
            byte[] rec = createLogRecord("record" + i, i + 100);
            int lsn = lm.append(rec);
            System.out.print(lsn + " ");
        }
        System.out.println();
    }

    private static byte[] createLogRecord(String s, int n) {
        int npos = PageObject.maxStringLength(s.length());
        byte[] b = new byte[npos + Integer.BYTES];
        PageObject p = new PageObject(b);
        p.setString(s, 0);
        p.setInt(n, npos);
        return b;
    }
}
