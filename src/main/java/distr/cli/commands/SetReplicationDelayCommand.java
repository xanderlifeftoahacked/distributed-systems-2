package distr.cli.commands;

import distr.cli.CliState;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "setReplicationDelayMs")
public final class SetReplicationDelayCommand extends BaseCommand {
    @Parameters(index = "0")
    private int minMs;

    @Parameters(index = "1")
    private int maxMs;

    @Override
    public void run() {
        if (minMs < 0 || maxMs < 0 || maxMs < minMs) {
            System.err.println("BAD_REQUEST");
            return;
        }
        CliState state = loadState();
        state.setDelayRange(minMs, maxMs);
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

