package ru.mail.polis.ivanov;

import one.nio.http.*;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import java.io.IOException;

public class KVServiceImpl extends HttpServer implements KVService {

    private final String ID = "id=";

    @NotNull
    private final KVDao kvDao;

    public KVServiceImpl(@NotNull HttpServerConfig config, @NotNull KVDao kvDao) throws IOException {
        super(config);
        this.kvDao = kvDao;
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
    public void entity(Request request, HttpSession session) throws IOException {

        String key = request.getParameter(ID);

        if (key == null || key.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        switch (request.getMethod()) {

            case Request.METHOD_GET:
                session.sendResponse(new Response(Response.OK, kvDao.get(key.getBytes())));
                break;

            case Request.METHOD_PUT:
                kvDao.upsert(key.getBytes(), request.getBody());
                session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                break;

            case Request.METHOD_DELETE:
                kvDao.remove(key.getBytes());
                session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                break;

            default:
                session.sendError(Response.BAD_REQUEST, null);
                break;
        }

    }


}