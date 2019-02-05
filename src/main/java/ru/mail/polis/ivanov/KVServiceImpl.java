package ru.mail.polis.ivanov;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.ivanov.handler.HandlerDelete;
import ru.mail.polis.ivanov.handler.HandlerGet;
import ru.mail.polis.ivanov.handler.HandlerPut;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class KVServiceImpl extends HttpServer implements KVService {
    public static final int INTERNAL_ERROR = 500;
    @NotNull
    private final KVDao kvDao;
    private final String DEFAULT_REPLICAS;
    private final Map<String, HttpClient> nodes;
    private final Logger logger = Logger.getLogger(KVServiceImpl.class);
    private final HandlerDelete handlerDelete;
    private final HandlerGet handlerGet;
    private final HandlerPut handlerPut;
    private String[] topology;
    private String me;

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

        handlerDelete = new HandlerDelete(kvDao, me, nodes);
        handlerGet = new HandlerGet(kvDao, me, nodes);
        handlerPut = new HandlerPut(kvDao, me, nodes);

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
                        session.sendResponse(handlerGet.get(id));
                    } else {
                        session.sendResponse(handlerGet.proxiedGET(
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
                        session.sendResponse(handlerPut.put(id, request.getBody()));
                    } else {
                        session.sendResponse(handlerPut.proxiedPUT(
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
                        session.sendResponse(handlerDelete.delete(id));
                    } else {
                        session.sendResponse(handlerDelete.proxiedDELETE(id, replicasDef.getAck(), getNodes(id, replicasDef.getFrom())));
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


    private List<String> getNodes(String key, int length) {
        List<String> clients = new ArrayList<>();
        int firstNodeId = (key.hashCode() & Integer.MAX_VALUE) % topology.length;
        for (int i = 0; i < length; i++) {
            clients.add(topology[(firstNodeId + i) % topology.length]);
        }
        return clients;
    }

}