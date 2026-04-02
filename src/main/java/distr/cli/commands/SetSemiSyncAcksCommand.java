package distr.cli.commands;

import distr.cli.CliState;
import distr.common.ReplicationMode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "setSemiSyncAcks")
public final class SetSemiSyncAcksCommand extends BaseCommand {
    @Parameters(index = "0")
    private int acks;

    @Override
    public void run() {
        CliState state = loadState();
        state.setSemiSyncAcks(acks);
        if (state.getReplicationMode() == ReplicationMode.SEMI_SYNC) {
            if (acks < 1 || acks > state.getRf() - 1) {
                System.err.println("BAD_REQUEST");
                return;
            }
        }
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

