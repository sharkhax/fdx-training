package com.drobot.cfunc.exception;

public class TriggerException extends Exception {

    public TriggerException() {
        super();
    }

    public TriggerException(String message) {
        super(message);
    }

    public TriggerException(String message, Throwable cause) {
        super(message, cause);
    }

    public TriggerException(Throwable cause) {
        super(cause);
    }
}
