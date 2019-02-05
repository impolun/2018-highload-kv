package ru.mail.polis.ivanov.handler;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.apache.log4j.Logger;
import ru.mail.polis.KVDao;
import ru.mail.polis.ivanov.Value;
import ru.mail.polis.ivanov.ValueSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static ru.mail.polis.ivanov.KVServiceImpl.INTERNAL_ERROR;


public class HandlerPut {

    private static final Logger logger = Logger.getLogger(HandlerPut.class);
    private final KVDao kvDao;
    private final String me;
    private final Map<String, HttpClient> nodes;

    public HandlerPut(KVDao kvDao, String me, Map<String, HttpClient> nodes) {
        this.kvDao = kvDao;
        this.me = me;
        this.nodes = nodes;
    }

    public Response put(String id, byte[] body) {
        try {
            kvDao.upsert(id.getBytes(), ValueSerializer.INSTANCE.serialize(new Value(body)));
            logger.info("PUT id=" + id);
        } catch (IOException ex) {
            logger.info(ex);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    public Response proxiedPUT(String id, byte[] body, int ack, List<String> from) {
        int myAck = 0;
        for (String node : from) {
            if (node.equals(me)) {
                try {
                    kvDao.upsert(id.getBytes(), ValueSerializer.INSTANCE.serialize(new Value(body, System.currentTimeMillis())));
                    myAck++;
                } catch (IOException ex) {
                    logger.info(ex);
                }
            } else {
                try {
                    final Response response = nodes.get(node).put("/v0/entity?id=" + id, body, "proxied: true");
                    if (response.getStatus() != INTERNAL_ERROR) {
                        myAck++;
                    }
                } catch (Exception ex) {
                    logger.info(ex);
                }
            }
        }
        if (myAck >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

}
