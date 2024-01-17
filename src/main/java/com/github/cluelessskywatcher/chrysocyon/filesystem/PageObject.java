package com.github.cluelessskywatcher.chrysocyon.filesystem;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 *  Class for representing pages of data in memory. Each PageObject
 *  consists of a ByteBuffer that stores bytes of data.
 */
public class PageObject {
    private ByteBuffer buffer;
    private static final Charset CC_CHARSET = StandardCharsets.US_ASCII;

    /**
     * Creates an empty PageObject with given blocksize.
     * @param blockSize the size of the page block
     */
    public PageObject(int blockSize) {
        buffer = ByteBuffer.allocateDirect(blockSize);
    }

    /**
     * Creates a PageObject from a given array of bytes
     * @param b an array of bytes
     */
    public PageObject(byte[] b) {
        buffer = ByteBuffer.wrap(b);
    }

    /**
     * Given a particular page offset, return the integer stored
     * at that location. Make sure that the given offset contains an integer
     * or it may produce unexpected results
     * @param offset the offset where an integer is (supposed to be) stored
     * @return the integer at the specified location
     */
    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    /**
     * Given an integer and a byte offset, store the integer at that particular
     * offset
     * @param n the integer to be stored
     * @param offset the offset of the integer to be stored
     */
    public void setInt(int n, int offset) {
        buffer.putInt(offset, n);
    }

    /**
     * Given a byte offset, fetch an integer that stores the number
     * of bytes to be fetched, then fetch that many bytes
     * @param offset the location where the byte length is stored
     * @return the byte array of the length specified by the integer in the offset
     */
    public byte[] getBytes(int offset) {
        buffer.position(offset);
        int byteLength = buffer.getInt();
        
        byte[] b = new byte[byteLength];
        buffer.get(b);

        return b;
    }

    public void setBytes(byte[] b, int offset) {
        buffer.position(offset);
        buffer.putInt(b.length);
        buffer.put(b);
    }

    public String getString(int offset) {
        byte[] bytes = getBytes(offset);
        return new String(bytes, CC_CHARSET);
    }

    public void setString(String s, int offset) {
        byte[] bytes = s.getBytes(CC_CHARSET);
        setBytes(bytes, offset);
    }

    /**
     * Given the length of a string, calculate the maximum number of bytes
     * required to store that string, as per the supported Charset. E.g. For
     * the string "ABC" length is 3, and let's say the Charset supports 1 byte
     * per character, then this function returns 4 (number of bytes to store the string length) +
     * 3 (length) * 1 (byte per character)
     *  = 7
     * @param len length of the actual string
     * @return max number of bytes required to store this string
     */
    public static int maxStringLength(int len) {
        float charBytes = CC_CHARSET.newEncoder().maxBytesPerChar();

        // Additional integer bytes are added to denote the string length
        return Integer.SIZE + (len * (int) charBytes);
    }

    ByteBuffer getContentBuffer() {
        buffer.position(0);
        return buffer;
    }
}
