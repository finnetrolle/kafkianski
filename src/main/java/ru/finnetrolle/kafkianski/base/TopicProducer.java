package ru.finnetrolle.kafkianski.base;

/**
 * Created by finnetrolle on 05.02.2016.
 */
public interface TopicProducer<MSG> {

    void send(MSG message);



}
