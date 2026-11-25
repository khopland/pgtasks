package io.github.khopland.pgtasks.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.test.PostgresDatabase;

/**
 * Forwards TCP to the database, and can stop forwarding on the links it has without closing them:
 * a connection the network dropped silently, as a failover or a NAT timing out an idle flow does.
 */
final class TcpProxy implements AutoCloseable {

    private final ServerSocket server;
    private final List<Link> links = new CopyOnWriteArrayList<>();

    TcpProxy(String host, int port) throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(() -> {
            while (!server.isClosed()) {
                try {
                    Socket client = server.accept();
                    Link link = new Link(client, new Socket(host, port));
                    links.add(link);
                    link.start();
                } catch (IOException e) {
                    return;
                }
            }
        });
    }

    /** A proxy in front of the database. */
    static TcpProxy to(PostgresDatabase database) throws IOException {
        URI target = URI.create(database.jdbcUrl().substring("jdbc:".length()));
        return new TcpProxy(target.getHost(), target.getPort());
    }

    /** The database's JDBC URL, through this proxy. */
    String jdbcUrl(PostgresDatabase database) {
        URI target = URI.create(database.jdbcUrl().substring("jdbc:".length()));
        return database.jdbcUrl().replace(target.getHost() + ":" + target.getPort(), "127.0.0.1:" + port());
    }

    int port() {
        return server.getLocalPort();
    }

    /** Stops forwarding on every link open now, in both directions, without closing it. New links work. */
    void blackholeOpenLinks() {
        links.forEach(link -> link.blackholed = true);
    }

    @Override
    public void close() throws IOException {
        server.close();
        for (Link link : links) {
            link.client.close();
            link.upstream.close();
        }
    }

    private static final class Link {

        final Socket client;
        final Socket upstream;
        volatile boolean blackholed;

        Link(Socket client, Socket upstream) {
            this.client = client;
            this.upstream = upstream;
        }

        void start() {
            Thread.ofVirtual().start(() -> pump(client, upstream));
            Thread.ofVirtual().start(() -> pump(upstream, client));
        }

        private void pump(Socket from, Socket to) {
            byte[] buffer = new byte[8192];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    if (!blackholed) {
                        out.write(buffer, 0, read);
                        out.flush();
                    }
                }
            } catch (IOException ignored) {
                // closed
            }
        }
    }
}
