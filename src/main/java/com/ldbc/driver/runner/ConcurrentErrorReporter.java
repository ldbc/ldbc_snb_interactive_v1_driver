package com.ldbc.driver.runner;

import com.ldbc.driver.util.Tuple;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO test
public class ConcurrentErrorReporter {
    private final AtomicBoolean errorEncountered = new AtomicBoolean(false);
    private final ConcurrentLinkedQueue<Tuple.Tuple2<String, String>> errorMessages = new ConcurrentLinkedQueue<Tuple.Tuple2<String, String>>();

    public void reportError(Object caller, String errMsg) {
        errorMessages.add(Tuple.tuple2(whoAmI(caller), errMsg));
        errorEncountered.set(true);
    }

    public boolean errorEncountered() {
        return errorEncountered.get();
    }

    public ConcurrentLinkedQueue<Tuple.Tuple2<String, String>> errorMessages() {
        return errorMessages;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Tuple.Tuple2<String, String> errMsg : errorMessages) {
            sb.append("\n\t* * * * * * *\n");
            sb.append("SOURCE:\t").append(errMsg._1()).append("\n");
            sb.append("ERROR:\t").append(errMsg._2());
        }
        sb.append("\n\t* * * *");
        return sb.toString();
    }

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
}
