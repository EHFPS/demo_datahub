package com.bayer.datahub;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class WebMockServerBaseTest {
    protected final PathDispatcher pathDispatcher = new PathDispatcher();
    protected final MockWebServer server = new MockWebServer();
    protected final String baseUrl = server.url("").toString();

    {
        server.setDispatcher(pathDispatcher);
    }

    @AfterEach
    @SuppressWarnings("unused")
    private void stopServer() throws IOException {
        server.shutdown();
    }

    protected void addPathResponse(String path, String body) {
        pathDispatcher.addPathResponse(path, new MockResponse().setBody(body));
    }

    protected void addPathResponse(String path, byte[] body) {
        var buffer = new Buffer();
        buffer.write(body);
        pathDispatcher.addPathResponse(path, new MockResponse().setBody(buffer));
    }

    protected void addPathResponse(String path) {
        pathDispatcher.addPathResponse(path, new MockResponse());
    }

    protected void addPathResponse(String path, MockResponse mockResponse) {
        pathDispatcher.addPathResponse(path, mockResponse);
    }

    protected void addPathResponse(String path, String headerName, String headerValue) {
        pathDispatcher.addPathResponse(path, new MockResponse().addHeader(headerName, headerValue));
    }

    private static class PathDispatcher extends Dispatcher {
        private final Map<String, MockResponse> pathToResponse = new HashMap<>();

        @NotNull
        @Override
        public MockResponse dispatch(RecordedRequest recordedRequest) {
            var path = recordedRequest.getPath();
            var mockResponse = pathToResponse.get(path);
            if (mockResponse == null) {
                mockResponse = new MockResponse()
                        .setResponseCode(404)
                        .setBody("MockResponse for path is not set: " + path);
            }
            return mockResponse;
        }

        public void addPathResponse(String path, MockResponse mockResponse) {
            path = addLeadingWithSlash(path);
            pathToResponse.put(path, mockResponse);
        }

        private static String addLeadingWithSlash(String path) {
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            return path;
        }
    }
}