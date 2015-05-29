package de.jeha.s3pt;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * @author jenshadlich@googlemail.com
 */
public class OperationResult {

    private final DescriptiveStatistics statistics;

    public OperationResult(DescriptiveStatistics statistics) {
        this.statistics = statistics;
    }

    public DescriptiveStatistics getStatistics() {
        return statistics;
    }
}
