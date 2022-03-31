package de.jeha.s3pt.operations.util;

import org.slf4j.Logger;

public class ProgressLogger {
    public static void logProgress(Logger logger, int i, int iMax) {
        final int iPeriod = Math.min(iMax / 10, 1000);
        if (i > 0 && i % iPeriod == 0) {
            logger.info("Progress: {} of {}", i, iMax);
        }
    }

}
