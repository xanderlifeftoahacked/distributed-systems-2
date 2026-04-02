package distr.cli.commands;

import distr.cli.CliState;
import distr.common.Topology;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "setTopology")
public final class SetTopologyCommand extends BaseCommand {
    @Parameters(index = "0")
    private String topology;

    @Override
    public void run() {
        CliState state = loadState();
        Topology parsed = Topology.fromString(topology);
        if (parsed == null) {
            System.err.println("INVALID_TOPOLOGY");
            return;
        }
        state.setTopology(parsed);
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

