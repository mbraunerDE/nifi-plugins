package de.mbrauner.nifiplugins.processors;

import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;

import javax.swing.text.html.Option;
import java.security.PublicKey;
import java.util.*;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "list", "sftp", "remote", "ingest", "source", "input", "files" })
@CapabilityDescription(
    "Performs a listing of the files residing on an SFTP server. For each file that is found on the remote server, a new FlowFile will be created with the filename attribute "
        + "set to the name of the file on the remote server. This can then be used in conjunction with FetchSFTP in order to fetch those files.")
@WritesAttributes({
    @WritesAttribute(attribute = "sftp.remote.host", description = "The hostname of the SFTP Server"),
    @WritesAttribute(attribute = "sftp.remote.port", description = "The port that was connected to on the SFTP Server"),
    @WritesAttribute(attribute = "sftp.listing.user", description = "The username of the user that performed the SFTP Listing"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the SFTP Server"),
    @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the SFTP Server from which the file was pulled"),
    @WritesAttribute(attribute = "directory", description = "The name of the directory on the SFTP Server from which the file was pulled"),
})
@ReadsAttributes({ @ReadsAttribute(attribute = "sftp.remote.host", description = "The hostname of the SFTP Server") })
@Stateful(scopes = { Scope.CLUSTER }, description = "After performing a listing of files, the timestamp of the newest file is stored. "
    + "This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
    + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
public class ListSFTPWithInput extends AbstractProcessor implements VerifiableProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Output relation for flow files")
        .build();
    public static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Output relation for flow files")
        .build();

    public static final PropertyDescriptor SFTP_PORT = new PropertyDescriptor.Builder().name("SFTP_PORT")
        .displayName("sftp port")
        .description("port for sftp connection")
        .defaultValue("22")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NUMBER_VALIDATOR)
        .build();
    public static final PropertyDescriptor SFTP_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder().name("SFTP_CONNECTION_TIMEOUT")
        .displayName("sftp connection timeout")
        .description("timeout to establish connection in milliseconds")
        .defaultValue("5000")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NUMBER_VALIDATOR)
        .build();

    public static final PropertyDescriptor STRICT_HOST_KEY_CHECKING = new PropertyDescriptor.Builder()
        .name("Strict Host Key Checking")
        .description("Indicates whether or not strict enforcement of hosts keys should be applied")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    public static final PropertyDescriptor SFTP_USERNAME = new PropertyDescriptor.Builder().name("SFTP_USERNAME")
        .displayName("sftp username")
        .description("username for sftp connection")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor SFTP_HOSTNAME = new PropertyDescriptor.Builder().name("sftp.remote.host")
        .displayName("sftp hostname")
        .description("hostname for sftp connection")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor SFTP_PASSWORD = new PropertyDescriptor.Builder().name("SFTP_PASSWORD")
        .displayName("sftp password")
        .description("password for sftp connection")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor SFTP_REMOTE_DIR = new PropertyDescriptor.Builder().name("SFTP_REMOTE_DIR")
        .displayName("sftp remote directory")
        .description("remote directory where the files are located to download")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor SFTP_FILE_FILTER = new PropertyDescriptor.Builder().name("SFTP_FILE_FILTER")
        .displayName("remote file filter")
        .description("filter files located in remote directory, used regex")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships = Collections.unmodifiableSet(relationships);

        descriptors = new ArrayList<>();
        descriptors.add(SFTP_USERNAME);
        descriptors.add(SFTP_PASSWORD);
        descriptors.add(SFTP_PORT);
        descriptors.add(SFTP_CONNECTION_TIMEOUT);
        descriptors.add(SFTP_REMOTE_DIR);
        descriptors.add(SFTP_FILE_FILTER);
        descriptors.add(SFTP_HOSTNAME);
        descriptors.add(STRICT_HOST_KEY_CHECKING);
        descriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private String getProperty(ProcessContext context, PropertyDescriptor descriptor, FlowFile ff) {
        if (context.getProperty(descriptor).isExpressionLanguagePresent()) {
            return context.getProperty(descriptor).evaluateAttributeExpressions(ff).getValue();
        } else {
            return context.getProperty(descriptor).getValue();
        }
    }

    @Override public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile ff = session.get();
        if (ff == null) {
            ff = session.create();
            session.getProvenanceReporter().create(ff, "created by " + this.getClass().getSimpleName() + " on it's own");
            getLogger().debug("use new flow file");
        } else {
            getLogger().debug("use flow file given by input");
        }

        String path = getProperty(context, SFTP_REMOTE_DIR, ff);
        String hostname = getProperty(context, SFTP_HOSTNAME, ff);
        String username = getProperty(context, SFTP_USERNAME, ff);
        String password = getProperty(context, SFTP_PASSWORD, ff);
        int port = Integer.parseInt(getProperty(context, SFTP_PORT, ff));
        boolean hostKeyCheck = "true".equals(getProperty(context, STRICT_HOST_KEY_CHECKING, ff));

        final SSHClient ssh = new SSHClient();
        try {
            if (!hostKeyCheck) {
                ssh.addHostKeyVerifier(new HostKeyVerifier() {

                    @Override public boolean verify(String hostname, int port, PublicKey key) {
                        return true;
                    }

                    @Override public List<String> findExistingAlgorithms(String hostname, int port) {
                        return null;
                    }
                });
            }
            ssh.connect(hostname, port);
            try {
                ssh.authPassword(username, password);
                final SFTPClient sftp = ssh.newSFTPClient();
                try {
                    List<RemoteResourceInfo> l = sftp.ls(path);
                    for (RemoteResourceInfo r : l) {
                        if (r.isRegularFile()) {
                            FlowFile output = session.create(ff);
                            Map<String, String> attributes = new HashMap<>();
                            attributes.put("sftp.remote.host", hostname);
                            attributes.put("sftp.remote.port", Integer.toString(port));
                            attributes.put("sftp.remote.user", username);
                            attributes.put("filename", r.getName());
                            attributes.put("path", r.getPath());
                            attributes.put("directory", r.getPath().replace(r.getName(), ""));
                            output = session.putAllAttributes(output, attributes);
                            session.transfer(output, SUCCESS);
                        }
                    }
                } finally {
                    sftp.close();
                }
            } finally {
                ssh.disconnect();
            }
            session.remove(ff);
        } catch (Throwable t) {
            getLogger().error(t.getMessage(), t);
            ff = session.penalize(ff);
            session.transfer(ff, FAILURE);
        }
    }

    @Override public List<ConfigVerificationResult> verify(ProcessContext context, ComponentLog verificationLogger, Map<String, String> attributes) {
        return null;
    }
}
