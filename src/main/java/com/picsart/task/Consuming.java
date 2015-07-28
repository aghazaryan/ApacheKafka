package com.picsart.task;

/**
 * Created by aghasighazaryan on 7/24/15.
 */

import com.mongodb.BasicDBObject;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class Consuming implements Callable<List<BasicDBObject>> {

    private static int count = 0;
    private KafkaStream stream;
    private int size;


    public Consuming(KafkaStream stream, int size) {

        this.stream = stream;
        this.size = size;
    }


    public List<BasicDBObject> call() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();


        List messages = new ArrayList<BasicDBObject>();
        try {
            while (it.hasNext() && count++ != size) {

                String str = new String(it.next().message());
                System.out.println(str);
                // Object o = com.mongodb.util.JSON.parse(str);
                // BasicDBObject basicDBObject = (BasicDBObject) o;
                //messages.add(basicDBObject);

                messages.add(new BasicDBObject("key", str));

            }
        } finally {

            return messages;
        }

    }
}