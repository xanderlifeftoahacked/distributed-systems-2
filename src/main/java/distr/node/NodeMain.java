package distr.node;

import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class NodeMain implements Runnable {
    private static final Logger LOG = Logger.getLogger(NodeMain.class.getName());

    @Option(names = {"-id", "--nodeId"}, description = "Node ID", required = true)
    private String nodeId;

    @Option(names = {"-host"}, description = "Host", defaultValue = "localhost")
    private String host;

    @Option(names = {"-port"}, description = "Port", defaultValue = "8080")
    private int port;

    @Override
    public void run() {
        NodeContext context = new NodeContext(nodeId, host, port);
        NodeServer server = new NodeServer(context);
        LOG.info("Starting node " + nodeId + " on " + host + ":" + port);
        try {
            server.start();
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Node failed", e);
            throw new RuntimeException(e);
        }
    }
}
