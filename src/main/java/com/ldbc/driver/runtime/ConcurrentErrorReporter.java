package com.ldbc.driver.runtime;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentErrorReporter {
    public static String stackTraceToString(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    // TODO include machine/process name
    public static String whoAmI(Object caller) {
        Thread myThread = Thread.currentThread();
        return String.format("%s (Thread: ID=%s, Name=%s, Priority=%s)",
                caller.getClass().getSimpleName(),
                myThread.getId(),
                myThread.getName(),
                myThread.getPriority());
    }

    public static String formatErrors(List<ErrorReport> errors) {
        StringBuilder sb = new StringBuilder();
        sb.append("- Error Log -");
        for (ErrorReport error : errors) {
            sb.append("\n\tSOURCE:\t").append(error.source());
            sb.append("\n\tERROR:\t").append(error.error());
        }
        return sb.toString();
    }

    private final AtomicBoolean errorEncountered = new AtomicBoolean(false);
    private final List<ErrorReport> errorMessages = new ArrayList<>();

    synchronized public void reportError(Object caller, String errMsg) {
        errorMessages.add(new ErrorReport(whoAmI(caller), errMsg));
        errorEncountered.set(true);
    }

    public boolean errorEncountered() {
        return errorEncountered.get();
    }

    public List<ErrorReport> errorMessages() {
        return errorMessages;
    }

    @Override
    public String toString() {
        if (errorMessages.isEmpty()) return "No Reported Errors";
        else return formatErrors(errorMessages());
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
