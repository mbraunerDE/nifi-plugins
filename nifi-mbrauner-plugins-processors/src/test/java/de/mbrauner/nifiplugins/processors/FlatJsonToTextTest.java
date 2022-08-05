package de.mbrauner.nifiplugins.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FlatJsonToTextTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(FlatJsonToText.class);
    }

    @Test
    public void testProcessor() {
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("{'key':'value','maike':'will lead','object':{'sub':'type'}}".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(JsonToAttribute.SUCCESS, 1);
        testRunner.assertAllFlowFilesTransferred(JsonToAttribute.SUCCESS);
        MockFlowFile ffReturn = testRunner.getFlowFilesForRelationship(JsonToAttribute.SUCCESS).get(0);
        assertThat(ffReturn.getContent()).isEqualToIgnoringWhitespace("key: value\n"
            + "maike: will lead\n"
            + "object: {\"sub\":\"type\"}");
    }

    @Test
    public void testProcessorArrayException() {
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("[{'key':'value','maike':'will lead','object':{'sub':'type'}}]".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(JsonToAttribute.FAILURE, 1);
        testRunner.assertAllFlowFilesTransferred(JsonToAttribute.FAILURE);
        MockFlowFile ffReturn = testRunner.getFlowFilesForRelationship(JsonToAttribute.FAILURE).get(0);
        assertThat(ffReturn).isNotNull();
        ffReturn.isContentEqual(ff.getContent());
    }

    @Test
    public void testProcessorNoJsonException() {
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("[{'key':'value','maike':'will lead','object':{'sub':'type'}}]".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(JsonToAttribute.FAILURE, 1);
        testRunner.assertAllFlowFilesTransferred(JsonToAttribute.FAILURE);
        MockFlowFile ffReturn = testRunner.getFlowFilesForRelationship(JsonToAttribute.FAILURE).get(0);
        assertThat(ffReturn).isNotNull();
        ffReturn.isContentEqual(ff.getContent());
    }
}
