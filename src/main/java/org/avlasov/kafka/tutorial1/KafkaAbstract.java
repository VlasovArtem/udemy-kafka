package org.avlasov.kafka.tutorial1;

import java.util.Properties;

public abstract class KafkaAbstract {

    protected Properties properties;

    public KafkaAbstract(String propertiesPath) {
        properties = PropertiesUtils.loadProperties(propertiesPath);
    }

    public abstract void perform(String topic);
}
