package com.alibaba.polardbx.server.encdb;

/**
 * @author pangzhaoxing
 */
public interface EncryptedResultSet {

    boolean isEncrypted(int columnIndex);

    boolean isMasked(int columnIndex);

}
