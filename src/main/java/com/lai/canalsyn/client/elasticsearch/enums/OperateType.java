package com.lai.canalsyn.client.elasticsearch.enums;


import com.lai.canalsyn.message.Dml;

/**
 * Es请求操作类型
 */
public enum OperateType {
    DOCINSERT,
    DOCDELETE,
    DOCUPDATE;
    public static OperateType chooseOperateType(Dml dml) {
        switch (dml.getType()) {
            case "INSERT":
                return DOCINSERT;
            case "UPDATE":
                return DOCUPDATE;
            case "DELETE":
                return DOCDELETE;
            default:
                return null;
        }
    }
}