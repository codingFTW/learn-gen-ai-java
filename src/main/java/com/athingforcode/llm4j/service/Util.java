package com.athingforcode.llm4j.service;

import java.util.Optional;
import java.util.function.Supplier;

public class Util {
    public static void printElapsedTime(long startTimeInMillis, String actionName) {
        var endTime = System.currentTimeMillis();
        long timeTakenMs = endTime - startTimeInMillis;
        double timeTakenSec = timeTakenMs / 1000.0;
        System.out.printf("Time taken to %s is %.2fs (%d ms)%n", actionName, timeTakenSec, timeTakenMs);
    }

    public static <T> Optional<T> retryOperation(Supplier<T> op, int maxRetry, String operationName) {
        T result = null;
        int retryCounter = 1;
        var done = false;
        while((retryCounter <= maxRetry) && !done) {
            try {
                result = op.get();
                done = true;
            }
            catch(Exception ex) {
                retryCounter++;
                if (retryCounter <= maxRetry) {
                    System.out.println("retry - " + retryCounter);
                }
                ex.printStackTrace();
            }
        }
        if (retryCounter > maxRetry) {
            throw new RuntimeException("Too many retries on " + operationName);
        }
        return Optional.ofNullable(result);
    }

    public static void retryVoidOperation(Runnable op, int maxRetry, String operationName) {
        int retryCounter = 1;
        var done = false;
        while((retryCounter <= maxRetry) && !done) {
            try {
                op.run();
                done = true;
            }
            catch(Exception ex) {
                retryCounter++;
                if (retryCounter <= maxRetry) {
                    System.out.println("retry-" + retryCounter);
                }
                ex.printStackTrace();
            }
        }
        if (retryCounter > maxRetry) {
            throw new RuntimeException("Too many retries on " + operationName);
        }
    }
}
