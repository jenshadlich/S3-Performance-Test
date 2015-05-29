package de.jeha.s3pt.operations;

import de.jeha.s3pt.OperationResult;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @author jenshadlich@googlemail.com
 */
public abstract class AbstractOperation implements Callable<OperationResult> {

    private final static Logger LOG = LoggerFactory.getLogger(AbstractOperation.class);

    private final DescriptiveStatistics statistics = new DescriptiveStatistics();

    protected DescriptiveStatistics getStatistics() {
        return statistics;
    }

    protected void logStatistics() {
        LOG.info("Request statistics:");
        LOG.info("min = {} ms", (int) statistics.getMin());
        LOG.info("max = {} ms", (int) statistics.getMax());
        LOG.info("avg = {} ms", (int) statistics.getGeometricMean());
        LOG.info("p50 = {} ms", (int) statistics.getPercentile(50));
        LOG.info("p75 = {} ms", (int) statistics.getPercentile(75));
        LOG.info("p95 = {} ms", (int) statistics.getPercentile(95));
        LOG.info("p98 = {} ms", (int) statistics.getPercentile(98));
        LOG.info("p99 = {} ms", (int) statistics.getPercentile(99));
    }

}
