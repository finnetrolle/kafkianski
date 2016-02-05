package ru.finnetrolle.kafkianski;

import kafka.producer.KeyedMessage;
import ru.finnetrolle.kafkianski.base.DefaultProducer;

import java.util.UUID;

import static ru.finnetrolle.kafkianski.base.DefaultProducer.Builder.Acks.FIRE_AND_FORGET;
import static ru.finnetrolle.kafkianski.base.DefaultProducer.Builder.Acks.WAIT_FOR_MASTER;
import static ru.finnetrolle.kafkianski.base.DefaultProducer.Builder.Acks.WAIT_FOR_SLAVES;

/**
 * Created by finnetrolle on 05.02.2016.
 */
public class Application {

    public static void main(String[] args) {
        test(10000, FIRE_AND_FORGET);
        test(10000, WAIT_FOR_MASTER);
        test(10000, WAIT_FOR_SLAVES);
    }

    private static void test(long count, DefaultProducer.Builder.Acks mode) {
        DefaultProducer producer = DefaultProducer.getBuilder()
                .setAck(mode)
                .setSerializer("kafka.serializer.StringEncoder")
                .addBroker("vsxo.ru", 9092)
                .addBroker("vsxo.ru", 9093)
                .build();
        long start = System.currentTimeMillis();
        String code = UUID.randomUUID().toString();
        for (int i = 0; i < count; ++i) {
            producer.send(new KeyedMessage<>("TutorialTopic", code + " >>> Hello from app! " + i));
        }
        long time = System.currentTimeMillis() - start;
        long throughput = count * 1000 / time;
        System.out.println("Results for " + mode.name() + " mode. Time = " + time + " ms, Throughput = " + throughput + " messages / second");
        producer.close();
    }

}
