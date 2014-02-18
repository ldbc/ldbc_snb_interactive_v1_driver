package com.ldbc.driver.runner;

import com.google.common.collect.Iterables;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentErrorReporter {
    public static String stackTraceToString(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    public static String whoAmI(Object caller) {
        Thread myThread = Thread.currentThread();
        return String.format("%s (Thread: ID=%s, Name=%s, Priority=%s)",
                caller.getClass().getSimpleName(),
                myThread.getId(),
                myThread.getName(),
                myThread.getPriority());
    }

    public static String formatErrors(Iterable<ErrorReport> errors) {
        StringBuilder sb = new StringBuilder();
        for (ErrorReport error : errors) {
            sb.append("\n\t* * * * * * *\n");
            sb.append("SOURCE:\t").append(error.source()).append("\n");
            sb.append("ERROR:\t").append(error.error());
        }
        sb.append("\n\t* * * *");
        return sb.toString();
    }

    private final AtomicBoolean errorEncountered = new AtomicBoolean(false);
    private final ConcurrentLinkedQueue<ErrorReport> errorMessages = new ConcurrentLinkedQueue<ErrorReport>();

    public void reportError(Object caller, String errMsg) {
        errorMessages.add(new ErrorReport(whoAmI(caller), errMsg));
        errorEncountered.set(true);
    }

    public boolean errorEncountered() {
        return errorEncountered.get();
    }

    public Iterable<ErrorReport> errorMessages() {
        return errorMessages;
    }

    public Iterable<ErrorReport> errorMessages(Class fromClass) {
        return Iterables.filter(errorMessages, fromClass);
    }

    @Override
    public String toString() {
        return formatErrors(errorMessages());
    }

    public static class ErrorReport {
        private final String source;
        private final String error;

        public ErrorReport(String source, String error) {
            this.source = source;
            this.error = error;
        }

        public String source() {
            return source;
        }

        public String error() {
            return error;
        }
    }
}
