package ru.finnetrolle.kafkianski.base;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by finnetrolle on 05.02.2016.
 */
public class DefaultProducer extends Producer<String, String> {

    private DefaultProducer(ProducerConfig config) {
        super(config);
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder {

        private final static String SETTINGS_BROKERS = "metadata.broker.list";
        private static final String SETTINGS_PARTITIONER = "partitioner.class";
        private static final String SETTINGS_SERIALIZER = "serializer.class";
        private static final String SETTINGS_ACK = "request.required.acks";


        private List<String> brokerList = new ArrayList<>();
        private String partitioner = null;
        private String serializer = null;
        private String ack = null;

        public DefaultProducer build() {
            if (brokerList.isEmpty()) {
                throw new IllegalArgumentException("Brokers list must not be empty");
            }

            Properties props = new Properties();
            setParam(props, SETTINGS_BROKERS, StringUtils.join(brokerList, ","));
            setParam(props, SETTINGS_PARTITIONER, partitioner);
            setParam(props, SETTINGS_SERIALIZER, serializer);
            setParam(props, SETTINGS_ACK, ack);

            ProducerConfig cfg = new ProducerConfig(props);
            return new DefaultProducer(cfg);
        }

        public Builder setPartitioner(String partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        public Builder setSerializer(String serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder setAck(Acks ack) {
            switch (ack) {
                case FIRE_AND_FORGET: this.ack = "0"; break;
                case WAIT_FOR_MASTER: this.ack = "1"; break;
                case WAIT_FOR_SLAVES: this.ack = "-1"; break;
                default: this.ack = "0";
            }
            return this;
        }

        public Builder addBroker(String connectionString) {
            brokerList.add(connectionString);
            return this;
        }

        public Builder addBroker(String hostname, int port) {
            brokerList.add(hostname + ":" + String.valueOf(port));
            return this;
        }

        private void setParam(Properties props, String paramName, String param) {
            if (!StringUtils.isEmpty(param)) {
                props.setProperty(paramName, param);
            }
        }

        public enum Acks {
            FIRE_AND_FORGET,
            WAIT_FOR_MASTER,
            WAIT_FOR_SLAVES
        }
    }

}
