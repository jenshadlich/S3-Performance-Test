package de.jeha.s3pt;

/**
 * @author jenshadlich@googlemail.com
 */
public enum Operation {

    CLEAR_BUCKET(false),
    CREATE_BUCKET(false),
    RANDOM_READ(true),
    UPLOAD_AND_READ(true),
    UPLOAD(true);

    private final boolean multiThreaded;

    Operation(boolean multiThreaded) {
        this.multiThreaded = multiThreaded;
    }

    public boolean isMultiThreaded() {
        return multiThreaded;
    }

}
