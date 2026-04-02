package distr.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class JsonUtil {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtil() {
    }

    public static ObjectNode object() {
        return MAPPER.createObjectNode();
    }

    public static ObjectNode parseObject(String json) throws JsonProcessingException {
        JsonNode node = MAPPER.readTree(json);
        if (node instanceof ObjectNode) {
            return (ObjectNode) node;
        }
        throw new JsonProcessingException("Expected JSON object") {
        };
    }

    public static String toJson(ObjectNode node) throws JsonProcessingException {
        return MAPPER.writeValueAsString(node);
    }
}

