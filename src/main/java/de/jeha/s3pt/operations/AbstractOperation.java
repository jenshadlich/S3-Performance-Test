package de.jeha.s3pt.operations;

import de.jeha.s3pt.OperationResult;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.concurrent.Callable;

/**
 * @author jenshadlich@googlemail.com
 */
public abstract class AbstractOperation implements Callable<OperationResult> {

    private final DescriptiveStatistics stats = new DescriptiveStatistics();

    protected DescriptiveStatistics getStats() {
        return stats;
    }

}
