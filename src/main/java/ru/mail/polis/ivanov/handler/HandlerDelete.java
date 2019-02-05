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

public class HandlerDelete {

    private static final Logger logger = Logger.getLogger(HandlerDelete.class);
    private final KVDao kvDao;
    private final String me;
    private final Map<String, HttpClient> nodes;

    public HandlerDelete(KVDao kvDao, String me, Map<String, HttpClient> nodes) {
        this.kvDao = kvDao;
        this.me = me;
        this.nodes = nodes;
    }

    public Response delete(String id) {
        try {
            Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED);
            kvDao.upsert(id.getBytes(), ValueSerializer.INSTANCE.serialize(val));
            logger.info("DELETE id=" + id);
        } catch (IOException ex) {
            logger.info(ex);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    public Response proxiedDELETE(String id, int ack, List<String> from) {
        int myAck = 0;
        for (String node : from) {
            if (node.equals(me)) {
                Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED);
                try {
                    byte[] ser = ValueSerializer.INSTANCE.serialize(val);
                    kvDao.upsert(id.getBytes(), ser);
                    myAck++;
                } catch (IOException ex) {
                    logger.info(ex);
                }
            } else {
                try {
                    final Response response = nodes.get(node).delete("/v0/entity?id=" + id, "proxied: true");
                    if (response.getStatus() != INTERNAL_ERROR) {
                        myAck++;
                    }
                } catch (Exception ex) {
                    logger.info(ex);
                }
            }
        }
        if (myAck >= ack) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }
}
