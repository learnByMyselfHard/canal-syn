package com.lai.canalsyn.client.elasticsearch.exception;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/21 16:09
 * @ Description :
 */
public class EsStorageException extends  Exception {
    public EsStorageException() {
    }
    public EsStorageException(String message) {
        super(message);
    }

    public EsStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsStorageException(Throwable cause) {
        super(cause);
    }

    public EsStorageException(String message, Throwable cause, boolean enableSuppression,
                            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
