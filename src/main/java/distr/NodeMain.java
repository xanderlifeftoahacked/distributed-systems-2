package distr;

import distr.node.NodeContext;
import distr.node.NodeServer;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@Command(name = "node", mixinStandardHelpOptions = true)
public final class NodeMain implements Runnable {
    private static final Logger LOG = Logger.getLogger(NodeMain.class.getName());

    @Option(names = {"--id"}, required = true)
    private String nodeId;

    @Option(names = {"--host"}, required = true)
    private String host;

    @Option(names = {"--port"}, required = true)
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

    public static void main(String[] args) {
        int code = new CommandLine(new NodeMain()).execute(args);
        if (code != 0) {
            System.exit(code);
        }
    }
}
