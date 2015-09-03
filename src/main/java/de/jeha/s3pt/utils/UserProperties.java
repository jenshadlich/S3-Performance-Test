package de.jeha.s3pt.utils;

/**
 * @author jenshadlich@googlemail.com
 */
public class UserProperties {

    public static UserPropertiesLoader fromHome() {
        return new UserHomePropertiesLoader();
    }

}
