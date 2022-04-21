package de.mbrauner.nifiplugins.processors;

import com.sun.mail.util.ASCIIUtility;
import jakarta.mail.Header;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeUtility;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.*;

import java.io.UnsupportedEncodingException;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DynamicProperty(name = "Relationship Name", value = "A Regular Expression", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    description = "Routes FlowFiles whose content matches the regular expression defined by Dynamic Property's value to the "
        + "Relationship defined by the Dynamic Property's key")
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's Regular Expression")
public class SendEmail extends PutEmail {

    private volatile Map<String, String> customHeaderMap = null;
    private static final Pattern ASCII_PATTERN = Pattern.compile("[\\x00-\\x7F]+");

    @Override public void onTrigger(ProcessContext context, ProcessSession session) {
        customHeaderMap = new HashMap<>();
        context.getProperties()
            .keySet()
            .stream()
            .filter(PropertyDescriptor::isDynamic)
            .forEach(k -> {
                if (ASCII_PATTERN.matcher(k.getName()).matches()) {
                    customHeaderMap.put("X-" + k.getName(), context.getProperties().get(k));
                } else {
                    customHeaderMap.put("X-" + k.getName().replaceAll("[^\\x00-\\x7F]",""), context.getProperties().get(k));
                }
            });
        super.onTrigger(context, session);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .required(false)
            .description("if name is not matching ASCII, non matching chars will be removed")
            .name(propertyDescriptorName)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    }

    @Override protected void send(Message msg) throws MessagingException {
        for (Map.Entry<String, String> e : customHeaderMap.entrySet()) {
            getLogger().info("add '{}' to mail with value '{}'", e.getKey(), e.getValue());
            try {
                msg.setHeader(e.getKey(), MimeUtility.encodeText(e.getValue()));
            } catch (UnsupportedEncodingException ex) {
                throw new MessagingException(ex.getMessage(), ex);
            }
        }
        super.send(msg);
    }
}
