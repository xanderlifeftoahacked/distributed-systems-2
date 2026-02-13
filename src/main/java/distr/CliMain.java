package distr;

import distr.cli.commands.AddNodeCommand;
import distr.cli.commands.BenchCommand;
import distr.cli.commands.DumpCommand;
import distr.cli.commands.GetCommand;
import distr.cli.commands.ListNodesCommand;
import distr.cli.commands.PutCommand;
import distr.cli.commands.RemoveNodeCommand;
import distr.cli.commands.SetLeaderCommand;
import distr.cli.commands.SetReplicationCommand;
import distr.cli.commands.SetReplicationDelayCommand;
import distr.cli.commands.SetRfCommand;
import distr.cli.commands.SetSemiSyncAcksCommand;
import distr.cli.commands.DeleteCommand;
import distr.cli.commands.ReplCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;

@Command(
        name = "cli",
        mixinStandardHelpOptions = true,
        subcommands = {
                AddNodeCommand.class,
                RemoveNodeCommand.class,
                ListNodesCommand.class,
                SetLeaderCommand.class,
                SetReplicationCommand.class,
                SetRfCommand.class,
                SetSemiSyncAcksCommand.class,
                SetReplicationDelayCommand.class,
                PutCommand.class,
                GetCommand.class,
                DumpCommand.class,
                DeleteCommand.class,
                BenchCommand.class,
                ReplCommand.class
        }
)
public final class CliMain implements Runnable {
    @Option(names = {"--state"}, description = "Path to CLI state file")
    private Path statePath;

    @Override
    public void run() {
        CommandLine cmd = new CommandLine(this);
        cmd.usage(System.out);
    }

    public Path statePath() {
        if (statePath != null) {
            return statePath;
        }
        return Path.of(System.getProperty("user.home"), ".distr", "cli-state.json");
    }

    public static void main(String[] args) {
        int code = new CommandLine(new CliMain()).execute(args);
        if (code != 0) {
            System.exit(code);
        }
    }
}
