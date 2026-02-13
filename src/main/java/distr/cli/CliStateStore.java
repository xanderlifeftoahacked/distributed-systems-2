package distr.cli;

import distr.common.JsonUtil;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class CliStateStore {
    private final Path path;

    public CliStateStore(Path path) {
        this.path = path;
    }

    public CliState load() {
        if (!Files.exists(path)) {
            return new CliState();
        }
        try {
            String content = Files.readString(path);
            if (content.isBlank()) {
                return new CliState();
            }
            ObjectNode root = JsonUtil.parseObject(content);
            return CliState.fromJson(root);
        } catch (IOException e) {
            return new CliState();
        }
    }

    public void save(CliState state) {
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            return;
        }
        try {
            String json = JsonUtil.toJson(state.toJson());
            Files.writeString(path, json);
        } catch (IOException e) {
            return;
        }
    }
}
