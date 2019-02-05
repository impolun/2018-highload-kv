package ru.mail.polis.ivanov.handler;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.apache.log4j.Logger;
import ru.mail.polis.KVDao;
import ru.mail.polis.ivanov.Value;
import ru.mail.polis.ivanov.ValueSerializer;

import java.io.IOException;
import java.util.*;

public class HandlerGet {

    private static final Logger logger = Logger.getLogger(HandlerGet.class);
    private final KVDao kvDao;
    private final String me;
    private final Map<String, HttpClient> nodes;

    public HandlerGet(KVDao kvDao, String me, Map<String, HttpClient> nodes) {
        this.kvDao = kvDao;
        this.me = me;
        this.nodes = nodes;
    }

    public Response proxiedGET(String id, int ack, List<String> from) {
        final List<Value> values = new ArrayList<>();
        for (String node : from) {
            if (node.equals(me)) {
                try {
                    values.add(ValueSerializer.INSTANCE.deserialize(kvDao.get(id.getBytes())));
                } catch (NoSuchElementException ex) {
                    logger.info(ex);
                    values.add(new Value(new byte[0], Long.MIN_VALUE, Value.State.UNKNOWN));
                } catch (IOException ex) {
                    logger.info(ex);
                }
            } else {
                try {
                    final Response response = nodes.get(node).get("/v0/entity?id=" + id, "proxied: true");
                    switch (response.getStatus()) {
                        case 200:
                            values.add(proxGetNewValue(response));
                            logger.info("proxiedGET ok" + ":" + response.getStatus());
                            break;
                        case 404:
                            values.add(proxGetNewValue(response));
                            logger.info("proxiedGET not found" + ":" + response.getStatus());
                            break;
                        case 500:
                            logger.info("proxiedGET bad answer" + ":" + response.getStatus());
                            break;
                        default:
                            logger.info("proxiedGET def" + ":" + response.getStatus());
                    }
                } catch (Exception ex) {
                    logger.info(ex);
                }
            }
        }
        if (values.size() >= ack) {
            Value max = values.stream()
                    .max(Comparator.comparingLong(Value::getTimestamp)).get();
            logger.info(max.getState());
            return max.getState() == Value.State.PRESENT ?
                    new Response(Response.OK, max.getData()) :
                    new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Value proxGetNewValue(Response response) {
        return new Value(response.getBody(),
                Long.parseLong(response.getHeader("Timestamp")),
                Integer.parseInt(response.getHeader("State")));
    }


    public Response get(String id) {
        Response response = null;
        try {
            logger.info("GET id=" + id);
            Value value = ValueSerializer.INSTANCE.deserialize(kvDao.get(id.getBytes()));
            response = new Response(Response.OK, value.getData());
            response.addHeader("Timestamp" + value.getTimestamp());
            response.addHeader("State" + value.getState().ordinal());
        } catch (NoSuchElementException ex) {
            response = new Response(Response.NOT_FOUND, Response.EMPTY);
            response.addHeader("Timestamp" + 0L);
            response.addHeader("State" + Value.State.UNKNOWN.ordinal());
        } catch (IOException ex) {
            logger.info(ex);
        }
        return response;
    }

}
