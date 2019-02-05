package ru.mail.polis.ivanov;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.abs;

public class KVServiceImpl extends HttpServer implements KVService {
    @NotNull
    private final KVDao kvDao;
    private String[] topology;
    private String me;
    private final String DEFAULT_REPLICAS;
    private final Map<String, HttpClient> nodes;
    private final Logger logger = Logger.getLogger(KVServiceImpl.class);
    private final int INTERNAL_ERROR = 500;

    public KVServiceImpl(@NotNull HttpServerConfig config, @NotNull KVDao kvDao, @NotNull Set<String> topology) throws IOException {
        super(config);
        this.kvDao = kvDao;
        this.topology = topology.toArray(new String[0]);
        me = "http://localhost:" + config.acceptors[0].port;
        logger.info("ME: " + me);
        nodes = topology.stream().collect(Collectors.toMap(
                o -> o,
                o -> new HttpClient(new ConnectionString(o))));
        DEFAULT_REPLICAS = (nodes.size() / 2 + 1) + "/" + nodes.size();
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendError(Response.BAD_REQUEST, null);
    }

    @Path("/v0/status")
    public void status(Request request, HttpSession session) throws IOException {
        if (request.getMethod() == Request.METHOD_GET) {
            session.sendResponse(Response.ok(Response.EMPTY));
        } else {
            session.sendError(Response.BAD_REQUEST, null);
        }
    }

    @Path("/v0/entity")
    public void entity(Request request, HttpSession session,
                       @Param("id=") String id, @Param("replicas=") String replicas) throws IOException {
        try {
            if (id == null || id.isEmpty()) {
                session.sendError(Response.BAD_REQUEST, null);
                return;
            }

            final Replicas replicasDef;

            if (replicas == null || replicas.isEmpty()) {
                replicasDef = new Replicas(DEFAULT_REPLICAS, nodes.size());
            } else {
                replicasDef = new Replicas(replicas, nodes.size());
            }

            final boolean isProxied = request.getHeader("proxied") != null;

            logger.info("REQUEST " + replicas + " proxied: " + isProxied);
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    logger.info("METHOD_GET");
                    if (isProxied) {
                        session.sendResponse(get(id));
                    } else {
                        session.sendResponse(proxiedGET(
                                id,
                                replicasDef.getAck(),
                                getNodes(id, replicasDef.getFrom())
                                )
                        );
                    }
                    break;

                case Request.METHOD_PUT:
                    logger.info("METHOD_PUT");
                    if (isProxied) {
                        session.sendResponse(put(id, request.getBody()));
                    } else {
                        session.sendResponse(proxiedPUT(
                                id,
                                request.getBody(),
                                replicasDef.getAck(),
                                getNodes(id, replicasDef.getFrom())
                                )
                        );
                    }
                    break;

                case Request.METHOD_DELETE:
                    logger.info("METHOD_DELETE");
                    if (isProxied) {
                        session.sendResponse(delete(id));
                    } else {
                        session.sendResponse(proxiedDELETE(id, replicasDef.getAck(), getNodes(id, replicasDef.getFrom())));
                    }
                    break;

                default:
                    logger.info("METHOD_DEFAULT");
                    session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            }
        } catch (IllegalArgumentException ex) {
            logger.info(ex);
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        } catch (NoSuchElementException ex) {
            logger.info(ex);
            session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
        } catch (Exception ex) {
            logger.info(ex);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private Response delete(String id) {
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

    private Response put(String id, byte[] body) {
        try {
            kvDao.upsert(id.getBytes(), ValueSerializer.INSTANCE.serialize(new Value(body)));
            logger.info("PUT id=" + id);
        } catch (IOException ex) {
            logger.info(ex);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response get(String id) {
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

    private List<String> getNodes(String key, int length) {
        List<String> clients = new ArrayList<>();
        int firstNodeId = (key.hashCode() & Integer.MAX_VALUE) % topology.length;
        for (int i = 0; i < length; i++) {
            clients.add(topology[(firstNodeId + i) % topology.length]);
        }
        return clients;
    }

    private Response proxiedGET(String id, int ack, List<String> from) {
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
                    switch (response.getStatus()){
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

    private Response proxiedPUT(String id, byte[] body, int ack, List<String> from) {
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

    private Response proxiedDELETE(String id, int ack, List<String> from) {
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