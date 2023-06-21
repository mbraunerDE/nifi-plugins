package de.mbrauner.nifiplugins.processors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.mbrauner.nifiplugins.processors.util.StringStreamCallback;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({"json", "attribute"})
@CapabilityDescription("Transfer all flat json values to text")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

public class FlatJsonToText extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Output relation for converted flow files")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Output relation for flow files without possible json converting")
            .build();

    private static final Gson gson = new GsonBuilder().create();

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile1 = session.get();
        if (flowFile1 != null) {
            try (InputStream is = session.read(flowFile1);
                 InputStreamReader isr = new InputStreamReader(is)) {
                JsonElement je = gson.fromJson(isr, JsonElement.class);
                if (je != null) {
                    if (!je.isJsonObject()) {
                        throw new Exception(je.getClass() + " is not supported as root element");
                    } else {
                        JsonObject jo = je.getAsJsonObject();
                        FlowFile flowFile2 = session.create(flowFile1);
                        StringBuilder content = new StringBuilder();
                        for (Map.Entry<String, JsonElement> e : jo.entrySet()
                                .stream()
                                .sorted(Map.Entry.comparingByKey())
                                .collect(Collectors.toList())) {
                            if (e.getValue().isJsonPrimitive()) {
                                content.append(e.getKey()).append(":\t").append(e.getValue().getAsString()).append("\n");
                            } else {
                                content.append(e.getKey()).append(":\t").append(gson.toJson(e.getValue())).append("\n");
                            }
                        }
                        flowFile2 = session.write(flowFile2, new StringStreamCallback(content.toString().trim()));
                        session.transfer(flowFile2, SUCCESS);
                    }
                } else {
                    throw new NullPointerException(String.format("cannot handle <%s>", IOUtils.toString(is, StandardCharsets.UTF_8)));
                }
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);
                session.transfer(flowFile1, FAILURE);
                flowFile1 = null;
            } finally {
                if (flowFile1 != null) {
                    session.remove(flowFile1);
                }
            }
        }
    }

}
