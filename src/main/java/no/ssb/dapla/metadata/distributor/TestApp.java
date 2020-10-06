package no.ssb.dapla.metadata.distributor;

import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;

import java.util.concurrent.TimeUnit;

public class TestApp {

    public static void main(String[] args) throws Exception {
        WebServer webServer = WebServer
                .create(Routing.builder()
                        .any((req, res) -> res.send("It works!"))
                        .build())
                .start()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);

        System.out.println("Server started at: http://localhost:" + webServer.port());
    }
}
