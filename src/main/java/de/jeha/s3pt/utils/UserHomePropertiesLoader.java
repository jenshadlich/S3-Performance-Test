package de.jeha.s3pt.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author jenshadlich@googlemail.com
 */
class UserHomePropertiesLoader implements UserPropertiesLoader {

    private static final Logger LOG = LoggerFactory.getLogger(UserHomePropertiesLoader.class);

    @Override
    public Properties load(String context) throws IOException {
        Properties properties = new Properties();

        final String userHome = System.getProperty("user.home");
        LOG.debug("user.home = '{}'", userHome);

        final String cfgFileName = userHome + File.separator + "." + context + ".cfg";
        final File cfgFile = new File(cfgFileName);

        if (cfgFile.exists()) {
            for (String line : FileUtils.readLines(cfgFile)) {
                String parts[] = StringUtils.split(line.trim(), "=", 2);
                if (parts.length != 2) {
                    LOG.warn("Ignore line '{}', invalid format", line);
                    continue;
                }
                final String key = parts[0];
                final String value = parts[1];
                properties.setProperty(key, value);

                LOG.debug("found setting for key ='{}', value = '{}'", key, value);
            }
        } else {
            LOG.warn("File '{}' does not exist", cfgFileName);
        }

        return properties;
    }

}
