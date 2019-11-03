package de.jeha.s3pt;

/**
 * @author jenshadlich@googlemail.com
 */
public enum Operation {

    CLEAR_BUCKET(false),
    CLEAR_BUCKET_PARALLEL(false, true),
    CREATE_BUCKET(false),
    CREATE_KEY_FILE(false),
    DELETE_BUCKET(false),
    RANDOM_GET(true),
    RANDOM_READ(true),
    RANDOM_READ_FIRST_BYTE(true),
    RANDOM_READ_METADATA(true),
    UPLOAD_AND_READ(true),
    UPLOAD(true);

    private final boolean multiThreaded;
    private final boolean multiThreadedInside;

    Operation(boolean multiThreaded) {
        this.multiThreaded = multiThreaded;
        this.multiThreadedInside = false;
    }
    Operation(boolean multiThreaded, boolean multiThreadedInside) {
        this.multiThreaded = multiThreaded;
        this.multiThreadedInside = multiThreadedInside;
    }

    public boolean isMultiThreaded() {
        return multiThreaded;
    }

    public boolean isMultiThreadedInside() {
        return multiThreadedInside;
    }
}
