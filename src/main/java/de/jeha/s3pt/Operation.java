package de.jeha.s3pt;

/**
 * @author jenshadlich@googlemail.com
 */
public enum Operation {

    UPLOAD(false),
    CLEAR_BUCKET(false),
    RANDOM_READ(true);

    private final boolean multiThreaded;

    Operation(boolean multiThreaded) {
        this.multiThreaded = multiThreaded;
    }

    public boolean isMultiThreaded() {
        return multiThreaded;
    }

}
