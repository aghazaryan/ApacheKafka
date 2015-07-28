package com.picsart.task;

import com.mongodb.BasicDBObject;

/**
 * Created by aghasighazaryan on 7/27/15.
 */
public class Main {

    public static void main(String[] args) {

        CHgitemInch cHgitemInch = new CHgitemInch("gr100","localhost:2181","test",4);

        for(BasicDBObject x :cHgitemInch.getMessages(4))
        {
            System.out.println(x.toString());

        }

    }

}
