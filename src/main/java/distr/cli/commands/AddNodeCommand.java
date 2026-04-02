package distr.cli.commands;

import distr.cli.CliState;
import distr.common.NodeRole;
import distr.common.NodeInfo;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "addNode")
public final class AddNodeCommand extends BaseCommand {
    @Parameters(index = "0")
    private String nodeId;

    @Parameters(index = "1")
    private String host;

    @Parameters(index = "2")
    private int port;

    @Option(names = {"--role"}, defaultValue = "follower")
    private String role;

    @Override
    public void run() {
        CliState state = loadState();
        NodeRole nodeRole = NodeRole.fromString(role);
        if (nodeRole == null) {
            System.err.println("BAD_REQUEST");
            return;
        }
        state.upsertNode(new NodeInfo(nodeId, host, port, nodeRole));
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

