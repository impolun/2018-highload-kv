package ru.mail.polis.ivanov;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class KVServiceImpl extends HttpServer implements KVService {

    @NotNull
    private final KVDao kvDao;
    private Replicas replicasDef;
    private String[] topology;
    private String me;
    private final ValueSerializer serializer;
    private final List<HttpClient> nodes = new ArrayList<>();

    public KVServiceImpl(@NotNull HttpServerConfig config, @NotNull KVDao kvDao, @NotNull Set<String> topology) throws IOException {
        super(config);
        this.kvDao = kvDao;
        this.topology = topology.toArray(new String[0]);
        me = "localhost:" + config.acceptors[0].port;
        topology.stream().forEach(node -> {
            HttpClient client = new HttpClient(new ConnectionString(node));
            nodes.add(client);
        });
        serializer = new ValueSerializer();
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
    public Response entity(Request request, HttpSession session,
                           @Param("id=") String id, @Param("replicas=") String replicas) throws IOException {

        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, null);
        }

        if (replicas == null || replicas.isEmpty()) {
            return new Response(Response.BAD_REQUEST, null);

        }

        replicasDef = new Replicas(replicas);

        boolean isProxied = request.getHeader("proxied") != null;

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                if (isProxied) {
                    return proxiedGET(id, replicasDef.getAck(), getNodes(id, replicasDef.getFrom()));
                } else {
                    return get(id);
                }

            case Request.METHOD_PUT:
                if (isProxied) {
                    return proxiedPUT(id, request.getBody(), replicasDef.getAck(), getNodes(id, replicasDef.getFrom()));
                } else {
                    return put(id, request.getBody());
                }

            case Request.METHOD_DELETE:
                if (isProxied) {
                    return proxiedDELETE(id, replicasDef.getAck(), getNodes(id, replicasDef.getFrom()));
                } else {
                    return delete(id);
                }

            default:
                return new Response(Response.BAD_REQUEST, null);

        }
    }

    private Response delete(String id) {
        try {
            Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED.ordinal());
            kvDao.upsert(id.getBytes(), serializer.serialize(val));
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        try {
            kvDao.upsert(id.getBytes(), serializer.serialize(new Value(body, System.currentTimeMillis())));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response get(String id) {
        Response response = null;
        try {
            Value value = serializer.deserialize(kvDao.get(id.getBytes()));
            response = new Response(Response.OK, value.getData());
            response.addHeader("timestamp" + value.getTimestamp());
            response.addHeader("state" + value.getState());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return response;
    }

    private List<String> getNodes(String key, int length) {
        ArrayList<String> clients = new ArrayList<>();
        //сгенерировать номер ноды на основе hash(key)
        int firstNodeId = (key.hashCode() & Integer.MAX_VALUE) % topology.length;
        clients.add(topology[firstNodeId]);
        //в цикле на увеличение добавить туда еще нод
        for (int i = 1; i < length; i++) {
            clients.add(topology[(firstNodeId + i) % topology.length]);
        }
        return clients;
    }

    private Response proxiedGET(String id, int ack, List<String> from) {

        List<Value> values = new ArrayList<>();

        for (String node : from
        ) {

            if (node.equals(me)) {
                try {
                    values.add(serializer.deserialize(kvDao.get(id.getBytes())));

                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            } else {
                try {
                    Response response = new HttpClient(new ConnectionString(node)).get("/v0/entity?id=" + id, "proxied: true");
                    values.add(new Value(response.getBody(),
                            Long.parseLong(response.getHeader("timestamp")),
                            Integer.parseInt(response.getHeader("state"))));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

        }

        List<Value> present = values.stream()
                .filter(v -> v.getState() == Value.State.PRESENT || v.getState() == Value.State.UNKNOWN)
                .collect(Collectors.toList());
        if (present.size() >= ack) {
            Value max = present.stream()
                    .max(Comparator.comparingLong(Value::getTimestamp)).get();
            return max.getState() == Value.State.UNKNOWN ?
                    new Response(Response.NOT_FOUND, Response.EMPTY) :
                    new Response(Response.OK, max.getData());
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);

    }

    private Response proxiedPUT(String id, byte[] body, int ack, List<String> from) {
        int myAck = 0;
        for (String node : from
        ) {
            if (node.equals(me)) {
                try {
                    kvDao.upsert(id.getBytes(), serializer.serialize(new Value(body, System.currentTimeMillis())));
                    myAck++;
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            } else {
                try {
                    Response response = (new HttpClient(new ConnectionString(node))).put("/v0/entity?id=" + id, body, "proxied: true");
                    if (response.getStatus() != 500) {
                        myAck++;
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        if (myAck >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        }

        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response proxiedDELETE(String id, int ack, List<String> from) {
        int myAck = 0;

        for (String node : from) {
            if (node.equals(me)) {
                Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED.ordinal());
                try {
                    byte[] ser = serializer.serialize(val);
                    kvDao.upsert(id.getBytes(), ser);
                    myAck++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED.ordinal());
                    byte[] value = serializer.serialize(val);
                    final Response response = new HttpClient(new ConnectionString(node)).put("/v0/entity?id=" + id, value, "proxied: true");
                    if (response.getStatus() != 500) {
                        myAck++;
                    }
                } catch (Exception e) {
                }
            }
        }

        if (myAck >= ack) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }
}