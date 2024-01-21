package com.github.cluelessskywatcher.chrysocyon.buffer;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

public class BufferPoolTest {
    public static void main(String[] args) {
        Chrysocyon db = new Chrysocyon("buffertest", 400, 3);
        BufferPoolManager bm = db.getBufferPoolManager();
        BufferObject buff1 = bm.pinBuffer(new BlockIdentifier("testfile", 1));

        PageObject p = buff1.getPage();

        int n = p.getInt(80);
        p.setInt(n + 1, 80); 
        buff1.setModified(1, 0);

        System.out.println("The new value is " + (n + 1));

        bm.unpinBuffer(buff1);
        // One of these pins will flush buff1 to disk:
        BufferObject buff2 = bm.pinBuffer(new BlockIdentifier("testfile", 2));
        BufferObject buff3 = bm.pinBuffer(new BlockIdentifier("testfile", 3));
        BufferObject buff4 = bm.pinBuffer(new BlockIdentifier("testfile", 4));
        
        bm.unpinBuffer(buff2);
        
        buff2 = bm.pinBuffer(new BlockIdentifier("testfile", 1));
        
        PageObject p2 = buff2.getPage();
        p2.setInt(9999, 80); // This modification
        buff2.setModified(1, 0); // won't get written to disk.
        bm.unpinBuffer(buff2);
    }
}