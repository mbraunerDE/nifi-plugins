package de.mbrauner.nifiplugins.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.standard.PutSFTP;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PutSFTPWithErrorMessage extends PutSFTP {

    /**
     * @param context
     * @param session
     * @see org.apache.nifi.processors.standard.PutFileTransfer#onTrigger(ProcessContext, ProcessSession)
     * but with customer exception handling
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        String hostname = context.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();

        final int maxNumberOfFiles = context.getProperty(FileTransfer.BATCH_SIZE).asInteger();
        int fileCount = 0;
        try (final SFTPTransfer transfer = getFileTransfer(context)) {
            do {
                //evaluate again inside the loop as each flowfile can have a different hostname
                hostname = context.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
                final String rootPath = context.getProperty(FileTransfer.REMOTE_PATH).evaluateAttributeExpressions(flowFile).getValue();
                final String workingDirPath;
                if (StringUtils.isBlank(rootPath)) {
                    workingDirPath = transfer.getHomeDirectory(flowFile);
                } else {
                    workingDirPath = transfer.getAbsolutePath(flowFile, rootPath);
                }

                final boolean rejectZeroByteFiles = context.getProperty(FileTransfer.REJECT_ZERO_BYTE).asBoolean();
                final ConflictResult conflictResult
                        = identifyAndResolveConflictFile(context.getProperty(FileTransfer.CONFLICT_RESOLUTION).getValue(), transfer, workingDirPath, flowFile, rejectZeroByteFiles, logger);

                if (conflictResult.isTransfer()) {
                    final StopWatch stopWatch = new StopWatch();
                    stopWatch.start();

                    beforePut(flowFile, context, transfer);
                    final FlowFile flowFileToTransfer = flowFile;
                    final AtomicReference<String> fullPathRef = new AtomicReference<>(null);
                    session.read(flowFile, new InputStreamCallback() {
                        @Override
                        public void process(final InputStream in) throws IOException {
                            try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                                if (workingDirPath != null && context.getProperty(SFTPTransfer.CREATE_DIRECTORY).asBoolean()) {
                                    transfer.ensureDirectoryExists(flowFileToTransfer, new File(workingDirPath));
                                }

                                fullPathRef.set(transfer.put(flowFileToTransfer, workingDirPath, conflictResult.getFileName(), bufferedIn));
                            }
                        }
                    });
                    afterPut(flowFile, context, transfer);

                    stopWatch.stop();
                    final String dataRate = stopWatch.calculateDataRate(flowFile.getSize());
                    final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                    logger.info("Successfully transferred {} to {} on remote host {} in {} milliseconds at a rate of {}",
                            new Object[]{flowFile, fullPathRef.get(), hostname, millis, dataRate});

                    String fullPathWithSlash = fullPathRef.get();
                    if (!fullPathWithSlash.startsWith("/")) {
                        fullPathWithSlash = "/" + fullPathWithSlash;
                    }
                    final String destinationUri = transfer.getProtocolName() + "://" + hostname + fullPathWithSlash;
                    session.getProvenanceReporter().send(flowFile, destinationUri, millis);
                }

                if (conflictResult.isPenalize()) {
                    flowFile = session.penalize(flowFile);
                }

                session.transfer(flowFile, conflictResult.getRelationship());
                session.commitAsync();
            } while (isScheduled()
                    && (getRelationships().size() == context.getAvailableRelationships().size())
                    && (++fileCount < maxNumberOfFiles)
                    && ((flowFile = session.get()) != null));
        } catch (final IOException e) {
            context.yield();
            logger.error("Unable to transfer {} to remote host {} due to {}", new Object[]{flowFile, hostname, e});
            flowFile = session.penalize(flowFile);
            Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
            attributes.put("ExceptionReport", e.getCause().getClass().getCanonicalName() + ": " +e.getMessage());
            session.transfer(session.putAllAttributes(session.penalize(flowFile), attributes), REL_FAILURE);
        } catch (final FlowFileAccessException e) {
            context.yield();
            logger.error("Unable to transfer {} to remote host {} due to {}", new Object[]{flowFile, hostname, e.getCause()});
            flowFile = session.penalize(flowFile);
            Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
            attributes.put("ExceptionReport", e.getCause().getClass().getCanonicalName() + ": " +e.getMessage());
            session.transfer(session.putAllAttributes(session.penalize(flowFile), attributes), REL_FAILURE);
        } catch (final ProcessException e) {
            context.yield();
            logger.error("Unable to transfer {} to remote host {} due to {}: {}; routing to failure", new Object[]{flowFile, hostname, e, e.getCause()});
            flowFile = session.penalize(flowFile);
            Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
            attributes.put("ExceptionReport", e.getCause().getClass().getCanonicalName() + ": " +e.getMessage());
            session.transfer(session.putAllAttributes(session.penalize(flowFile), attributes), REL_FAILURE);
        }
    }
}
