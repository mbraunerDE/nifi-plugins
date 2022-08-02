/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mbrauner.nifiplugins.processors;

import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class ListSFTPWithInputTest {

    @Rule
    public final FakeSftpServerRule sftpServer = new FakeSftpServerRule().addUser("nutzer", "passwort").setPort(12345);

    private TestRunner testRunner;

    @Before
    public void init() throws IOException {
        testRunner = TestRunners.newTestRunner(ListSFTPWithInput.class);
        testRunner.setProperty(ListSFTPWithInput.SFTP_USERNAME, "nutzer");
        testRunner.setProperty(ListSFTPWithInput.SFTP_PASSWORD, "passwort");
        testRunner.setProperty(ListSFTPWithInput.SFTP_PORT, "12345");
        testRunner.setProperty(ListSFTPWithInput.SFTP_REMOTE_DIR, "/directory/");
        testRunner.setProperty(ListSFTPWithInput.SFTP_FILE_FILTER, ".*");

        sftpServer.putFile("/directory/file.txt", "content of file", UTF_8);
    }

    @After
    public void after() throws IOException {
        sftpServer.deleteAllFilesAndDirectories();
    }

    @Test
    public void testProcessorWithHostname() {
        testRunner.setProperty(ListSFTPWithInput.SFTP_HOSTNAME, "127.0.0.1");
        testRunner.run(1);
        testRunner.assertTransferCount(ListSFTPWithInput.SUCCESS, 1);
        testRunner.assertAllFlowFilesTransferred(ListSFTPWithInput.SUCCESS);
        FlowFile ffReturn = testRunner.getFlowFilesForRelationship(ListSFTPWithInput.SUCCESS).get(0);
        Map<String, String> map = ffReturn.getAttributes();
        assertThat(map).isNotNull().isNotEmpty();
        assertThat(map)
            .containsEntry("sftp.remote.host", "127.0.0.1")
            .containsEntry("sftp.remote.port", "12345")
            .containsEntry("sftp.remote.user", "nutzer")
            .containsEntry("filename", "file.txt")
            .containsEntry("path", "/directory/file.txt")
            .containsEntry("directory", "/directory/");
        assertThat(testRunner.getProvenanceEvents()
            .stream().map(ProvenanceEventRecord::getEventType).distinct()).containsOnly(ProvenanceEventType.CREATE, ProvenanceEventType.FORK);
        assertThat(testRunner.getProvenanceEvents()
            .stream()
            .filter(provenanceEventRecord -> provenanceEventRecord.getEventType() == ProvenanceEventType.FORK)
            .count()).isEqualTo(1);
        assertThat(testRunner.getProvenanceEvents()
            .stream()
            .filter(provenanceEventRecord -> provenanceEventRecord.getEventType() == ProvenanceEventType.CREATE)
            .count()).isEqualTo(1);
    }

    @Test
    public void testProcessorWithInputHostname() {
        testRunner.setProperty(ListSFTPWithInput.SFTP_HOSTNAME, "${sftp.remote.host}");

        MockFlowFile ff = new MockFlowFile(456);
        ff.putAttributes(Collections.singletonMap("sftp.remote.host", "127.0.0.1"));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(ListSFTPWithInput.SUCCESS, 1);
        testRunner.assertAllFlowFilesTransferred(ListSFTPWithInput.SUCCESS);
        FlowFile ffReturn = testRunner.getFlowFilesForRelationship(ListSFTPWithInput.SUCCESS).get(0);
        Map<String, String> map = ffReturn.getAttributes();
        assertThat(map)
            .containsEntry("sftp.remote.host", "127.0.0.1")
            .containsEntry("sftp.remote.port", "12345")
            .containsEntry("sftp.remote.user", "nutzer")
            .containsEntry("filename", "file.txt")
            .containsEntry("path", "/directory/file.txt")
            .containsEntry("directory", "/directory/");
        assertThat(testRunner.getProvenanceEvents()
            .stream().map(ProvenanceEventRecord::getEventType).distinct()).containsOnly(ProvenanceEventType.DROP, ProvenanceEventType.FORK);
        assertThat(testRunner.getProvenanceEvents()
            .stream()
            .filter(provenanceEventRecord -> provenanceEventRecord.getEventType() == ProvenanceEventType.FORK)
            .count()).isEqualTo(1);
        assertThat(testRunner.getProvenanceEvents()
            .stream()
            .filter(provenanceEventRecord -> provenanceEventRecord.getEventType() == ProvenanceEventType.DROP)
            .count()).isEqualTo(1);
    }

    @Test
    public void testProcessorWithInputHostnameFailure() {
        testRunner.setProperty(ListSFTPWithInput.SFTP_HOSTNAME, "host");

        MockFlowFile ff = new MockFlowFile(123);
        ff.putAttributes(Collections.singletonMap("sftp.remote.host", "127.0.0.1"));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(ListSFTPWithInput.FAILURE, 1);
        testRunner.assertAllFlowFilesTransferred(ListSFTPWithInput.FAILURE);
        testRunner.assertPenalizeCount(1);
    }
}