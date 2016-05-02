package de.jeha.s3pt;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author jenshadlich@googlemail.com
 */
class TestResult {

    private static final Logger LOG = LoggerFactory.getLogger(TestResult.class);

    @JsonProperty
    private final int min;
    @JsonProperty
    private final int max;
    @JsonProperty
    private final int avg;
    @JsonProperty
    private final int p50;
    @JsonProperty
    private final int p75;
    @JsonProperty
    private final int p95;
    @JsonProperty
    private final int p98;
    @JsonProperty
    private final int p99;
    @JsonProperty
    private final double ops;

    TestResult(int min, int max, int avg, int p50, int p75, int p95, int p98, int p99, double ops) {
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.p50 = p50;
        this.p75 = p75;
        this.p95 = p95;
        this.p98 = p98;
        this.p99 = p99;
        this.ops = ops;
    }

    public static TestResult compute(List<OperationResult> results) {
        int min = (int) results.stream().mapToDouble(x -> x.getStats().getMin()).average().orElse(0.0);
        int max = (int) results.stream().mapToDouble(x -> x.getStats().getMax()).average().orElse(0.0);
        int avg = (int) results.stream().mapToDouble(x -> x.getStats().getGeometricMean()).average().orElse(0.0);
        int p50 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(50)).average().orElse(0.0);
        int p75 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(75)).average().orElse(0.0);
        int p95 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(95)).average().orElse(0.0);
        int p98 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(98)).average().orElse(0.0);
        int p99 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(99)).average().orElse(0.0);
        double ops = results.stream().mapToDouble(x -> x.getStats().getN() / x.getStats().getSum() * 1_000).sum();

        return new TestResult(min, max, avg, p50, p75, p95, p98, p99, ops);
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public int getAvg() {
        return avg;
    }

    public int getP50() {
        return p50;
    }

    public int getP75() {
        return p75;
    }

    public int getP95() {
        return p95;
    }

    public int getP98() {
        return p98;
    }

    public int getP99() {
        return p99;
    }

    public double getOps() {
        return ops;
    }

    /**
     * Log the test results.
     */
    public void log() {
        LOG.info("Result summary:");
        LOG.info("min = {} ms", min);
        LOG.info("max = {} ms", max);
        LOG.info("avg = {} ms", avg);
        LOG.info("p50 = {} ms", p50);
        LOG.info("p75 = {} ms", p75);
        LOG.info("p95 = {} ms", p95);
        LOG.info("p98 = {} ms", p98);
        LOG.info("p99 = {} ms", p99);
        LOG.info("throughput = {} operations/s", (int) ops);
    }

    /**
     * Write the test result to file as json.
     *
     * @param resultFileName name of the result file
     * @throws IOException
     */
    public void writeToFileAsJson(String resultFileName) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        String resultJson = mapper.writeValueAsString(this);
        FileUtils.writeStringToFile(new File(resultFileName), resultJson, StandardCharsets.UTF_8);
    }

}
