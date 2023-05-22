package de.mbrauner.nifiplugins.processors;

import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.standard.PutFileTransfer;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class PutSFTPWithErrorMessageTest {
    @Rule
    public final FakeSftpServerRule sftpServer = new FakeSftpServerRule().addUser("nutzer", "passwort").setPort(12345);

    private TestRunner testRunner;

    @Before
    public void init() throws IOException {
        testRunner = TestRunners.newTestRunner(PutSFTPWithErrorMessage.class);

        testRunner.setProperty(FileTransfer.HOSTNAME, "127.0.0.1");
        testRunner.setProperty(FileTransfer.USERNAME, "nutzer");
        testRunner.setProperty(FileTransfer.PASSWORD, "passwort");
        testRunner.setProperty(SFTPTransfer.PORT, "12345");
        testRunner.setProperty(FileTransfer.REMOTE_PATH, "/");

//        sftpServer.putFile("/directory/file.txt", "content of file", UTF_8);
    }

    @Test
    public void test() {
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("{'key':'value','maike':'will lead','object':{'sub':'type'}}".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(PutFileTransfer.REL_SUCCESS, 1);
        testRunner.assertAllFlowFilesTransferred(PutFileTransfer.REL_SUCCESS);
    }

    @Test
    public void testMissingDir() {
        testRunner.setProperty(FileTransfer.REMOTE_PATH, "/notExisting");

        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("{'key':'value','maike':'will lead','object':{'sub':'type'}}".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(PutFileTransfer.REL_FAILURE, 1);
        testRunner.assertAllFlowFilesTransferred(PutFileTransfer.REL_FAILURE);
        FlowFile ffReturn = testRunner.getFlowFilesForRelationship(PutFileTransfer.REL_FAILURE).get(0);
        Map<String, String> map = ffReturn.getAttributes();
        assertThat(map).isNotNull().isNotEmpty();
        assertThat(ffReturn.getAttributes())
                .containsKey("ExceptionReport")
                .containsKey("filename")//added by default
                .containsKey("path")//added by default
        ;
        assertThat(ffReturn.getAttribute("ExceptionReport")).startsWith("java.io.IOException: java.io.IOException: Unable to put content to /notExisting/").endsWith("mockFlowFile due to 2: No such file or directory");
    }
}
