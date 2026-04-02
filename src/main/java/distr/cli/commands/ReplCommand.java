package distr.cli.commands;

import distr.CliMain;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Command(
        name = "repl",
        description = "Interactive REPL for CLI commands",
        mixinStandardHelpOptions = true
)
public final class ReplCommand implements Runnable {
    @ParentCommand
    private CliMain parent;

    @Option(names = {"-p", "--prompt"}, description = "Prompt shown before each command")
    private String prompt = "distr> ";

    @Override
    public void run() {
        CommandLine cmd = new CommandLine(parent);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                System.out.print(prompt);
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                    break;
                }
                String[] tokens = tokenize(line);
                if (tokens == null) {
                    System.err.println("Parse error: unmatched quote or escape");
                    continue;
                }
                if (tokens.length == 0) {
                    continue;
                }
                if ("repl".equalsIgnoreCase(tokens[0])) {
                    System.err.println("Already in REPL. Use 'exit' to leave.");
                    continue;
                }
                int code = cmd.execute(tokens);
                if (code != 0) {
                    System.err.println("Command failed with exit code " + code);
                }
            }
        } catch (IOException e) {
            System.err.println("REPL terminated: " + e.getMessage());
        }
    }

    private static String[] tokenize(String line) {
        StringBuilder current = new StringBuilder();
        java.util.List<String> tokens = new java.util.ArrayList<>();
        boolean inQuotes = false;
        char quoteChar = 0;
        boolean escaping = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (escaping) {
                current.append(c);
                escaping = false;
                continue;
            }
            if (c == '\\') {
                escaping = true;
                continue;
            }
            if (inQuotes) {
                if (c == quoteChar) {
                    inQuotes = false;
                } else {
                    current.append(c);
                }
                continue;
            }
            if (c == '"' || c == '\'') {
                inQuotes = true;
                quoteChar = c;
                continue;
            }
            if (Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }
            current.append(c);
        }
        if (escaping || inQuotes) {
            return null;
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens.toArray(new String[0]);
    }
}
