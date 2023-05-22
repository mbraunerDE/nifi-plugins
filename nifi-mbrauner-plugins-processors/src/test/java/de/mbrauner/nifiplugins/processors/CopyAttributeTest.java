package de.mbrauner.nifiplugins.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CopyAttributeTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(CopyAttributes.class);
    }

    @Test
    public void testProcessorSingle() {
        testRunner.setProperty("sftp.remote.host", "${ip}");
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("{'maike':'will lead','key':'value','object':{'sub':'type'}}".getBytes(StandardCharsets.UTF_8));
        Map<String, String> map = new HashMap<>();
        map.put("sftp.remote.host", "falsch");
        map.put("ip", "richtig");
        ff.putAttributes(map);

        testRunner.assertValid();
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(JsonToAttribute.SUCCESS, 1);
        testRunner.assertAllFlowFilesTransferred(JsonToAttribute.SUCCESS);
        MockFlowFile ffReturn = testRunner.getFlowFilesForRelationship(JsonToAttribute.SUCCESS).get(0);
        assertThat(ffReturn.getAttributes()).containsEntry("sftp.remote.host", "richtig").containsEntry("ip", "richtig");
        assertThat(ff.getContent()).isEqualTo(new String(ff.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void testProcessorMulti() {
        testRunner.setProperty("sftp.remote.host", "Dies ist ein Test ${ip} mit mehreren Ersetzungen ${ip2}");
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("{'maike':'will lead','key':'value','object':{'sub':'type'}}".getBytes(StandardCharsets.UTF_8));
        Map<String, String> map = new HashMap<>();
        map.put("sftp.remote.host", "falsch");
        map.put("ip", "richtig");
        map.put("ip2", "richtig2");
        ff.putAttributes(map);

        testRunner.assertValid();
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(JsonToAttribute.SUCCESS, 1);
        testRunner.assertAllFlowFilesTransferred(JsonToAttribute.SUCCESS);
        MockFlowFile ffReturn = testRunner.getFlowFilesForRelationship(JsonToAttribute.SUCCESS).get(0);
        assertThat(ffReturn.getAttributes()).containsEntry("sftp.remote.host", "Dies ist ein Test richtig mit mehreren Ersetzungen richtig2").containsEntry("ip", "richtig").containsEntry("ip2", "richtig2");
        assertThat(ff.getContent()).isEqualTo(new String(ff.getData(), StandardCharsets.UTF_8));
    }
}
