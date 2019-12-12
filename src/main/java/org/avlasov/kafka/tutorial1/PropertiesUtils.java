package org.avlasov.kafka.tutorial1;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtils {

    public static Properties loadProperties(String path) {
        Properties properties = new Properties();
        try {
            properties.load(PropertiesUtils.class.getResourceAsStream(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }

}
