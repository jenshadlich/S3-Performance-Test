package de.jeha.s3pt.operations;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * @author jenshadlich@googlemail.com
 */
public abstract class AbstractOperation {

    private DescriptiveStatistics statistics = new DescriptiveStatistics();

    protected DescriptiveStatistics getStatistics() {
        return statistics;
    }

}
