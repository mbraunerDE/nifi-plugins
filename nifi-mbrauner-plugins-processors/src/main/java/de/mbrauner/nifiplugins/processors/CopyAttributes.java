package de.mbrauner.nifiplugins.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"copy", "attribute"})
@CapabilityDescription("Copies attributes from one in another attribtue")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class CopyAttributes extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Output relation for converted flow files")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Output relation for flow files without possible json converting")
            .build();

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
        FlowFile input = session.get();
        if (input != null) {
            try {
                Map<String, String> map = new HashMap<>(input.getAttributes());
                for (Map.Entry<PropertyDescriptor, String> e : context.getProperties().entrySet()) {
                    String newValue = e.getValue();
                    if (newValue.matches(".*\\$\\{.*?\\}.*")) {
                        Matcher m = Pattern.compile("\\$\\{.*?\\}").matcher(newValue);
                        while (m.find()) {
                            //String placeholder = m.group();
                            newValue = String.format("%s%s%s", newValue.substring(0, m.start()), map.get(newValue.substring(m.start() + 2, m.end() - 1)), newValue.substring(m.end()));
                            m.reset(newValue);
                        }
                    }
                    String oldValue = map.put(e.getKey().getName(), newValue);
                    getLogger().debug("change from {} to {}", oldValue, newValue);
                }
                FlowFile output = session.create(input);
                output = session.putAllAttributes(output, map);
                session.transfer(output, SUCCESS);
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);
                session.transfer(input, FAILURE);
                input = null;
            } finally {
                if (input != null) {
                    session.remove(input);
                }
            }
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(true)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

}
