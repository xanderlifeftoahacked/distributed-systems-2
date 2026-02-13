package distr.cli.commands;

import distr.cli.CliState;
import distr.common.ReplicationMode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "setRF")
public final class SetRfCommand extends BaseCommand {
    @Parameters(index = "0")
    private int rf;

    @Override
    public void run() {
        CliState state = loadState();
        if (rf < 1 || rf > state.getNodes().size()) {
            System.err.println("BAD_REQUEST");
            return;
        }
        state.setRf(rf);
        if (state.getReplicationMode() == ReplicationMode.SEMI_SYNC) {
            if (state.getSemiSyncAcks() < 1 || state.getSemiSyncAcks() > rf - 1) {
                System.err.println("BAD_REQUEST");
                return;
            }
        }
        saveState(state);
        broadcastClusterUpdate(state);
        System.out.println("OK");
    }
}

