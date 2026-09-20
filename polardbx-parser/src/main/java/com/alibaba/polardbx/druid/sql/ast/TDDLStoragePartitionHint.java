package com.alibaba.polardbx.druid.sql.ast;

/**
 * @author chenghui.lch
 */
public class TDDLStoragePartitionHint extends SQLCommentHint {

    protected String storagePrefixText;
    protected String contentText;

    public TDDLStoragePartitionHint(String storagePrefixText,
                                    String contentText) {
        super(storagePrefixText + contentText);
        this.storagePrefixText = storagePrefixText;
        this.contentText = contentText;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append("\n/*");
        sb.append(storagePrefixText);
        sb.append(" \n");
        String strTrim = contentText.trim();
        sb.append(strTrim);
        sb.append("\n*/");
        return sb.toString();
    }
}
