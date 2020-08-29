package com.lai.canalsyn.exception;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/21 16:09
 * @ Description :
 */
public class CanalException extends  Exception {
    public CanalException() {
    }
    public CanalException(String message) {
        super(message);
    }

    public CanalException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanalException(Throwable cause) {
        super(cause);
    }

    public CanalException(String message, Throwable cause, boolean enableSuppression,
                            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
