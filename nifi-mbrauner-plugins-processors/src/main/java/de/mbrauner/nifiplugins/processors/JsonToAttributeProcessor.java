package de.mbrauner.nifiplugins.processors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

@Tags({ "json", "attribute" })
@CapabilityDescription("Transfer all plain json values to attribute values, content is not modified")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class JsonToAttributeProcessor extends AbstractProcessor {

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
                if (!je.isJsonObject()) {
                    throw new Exception(je.getClass() + " is not supported as root element");
                } else {
                    JsonObject jo = je.getAsJsonObject();
                    FlowFile flowFile2 = session.create(flowFile1);
                    Map<String, String> map = new HashMap<>(jo.entrySet().size());
                    for (Map.Entry<String, JsonElement> e : jo.entrySet()) {
                        if (e.getValue().isJsonPrimitive()) {
                            map.put(e.getKey(), e.getValue().getAsString());
                        } else {
                            map.put(e.getKey(), gson.toJson(e.getValue()));
                        }
                    }
                    flowFile2 = session.putAllAttributes(flowFile2, map);
                    session.transfer(flowFile2, SUCCESS);
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
