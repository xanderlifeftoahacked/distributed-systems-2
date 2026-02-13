package distr.cli.commands;

import distr.cli.CliState;
import distr.common.ReplicationMode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "setReplication")
public final class SetReplicationCommand extends BaseCommand {
    @Parameters(index = "0")
    private String mode;

    @Override
    public void run() {
        CliState state = loadState();
        ReplicationMode replicationMode = ReplicationMode.fromString(mode);
        if (replicationMode == null) {
            System.err.println("BAD_REQUEST");
            return;
        }
        state.setReplicationMode(replicationMode);
        if (!validate(state)) {
            return;
        }
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }

    private boolean validate(CliState state) {
        int clusterSize = state.getNodes().size();
        if (state.getRf() > clusterSize) {
            System.err.println("BAD_REQUEST");
            return false;
        }
        if (state.getReplicationMode() == ReplicationMode.SEMI_SYNC) {
            int k = state.getSemiSyncAcks();
            if (k < 1 || k > state.getRf() - 1) {
                System.err.println("BAD_REQUEST");
                return false;
            }
        }
        return true;
    }
}

