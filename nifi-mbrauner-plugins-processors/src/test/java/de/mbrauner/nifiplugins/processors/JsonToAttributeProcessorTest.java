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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonToAttributeProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(JsonToAttributeProcessor.class);
    }

    @Test
    public void testProcessor() {
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("{'key':'value','maike':'will lead','object':{'sub':'type'}}".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(JsonToAttributeProcessor.SUCCESS, 1);
        testRunner.assertAllFlowFilesTransferred(JsonToAttributeProcessor.SUCCESS);
        FlowFile ffReturn = testRunner.getFlowFilesForRelationship(JsonToAttributeProcessor.SUCCESS).get(0);
        Map<String, String> map = ffReturn.getAttributes();
        assertThat(map).containsEntry("key", "value").containsEntry("maike", "will lead").containsEntry("object", "{\"sub\":\"type\"}");
    }

    @Test
    public void testProcessorArrayException() {
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("[{'key':'value','maike':'will lead','object':{'sub':'type'}}]".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(JsonToAttributeProcessor.FAILURE, 1);
        testRunner.assertAllFlowFilesTransferred(JsonToAttributeProcessor.FAILURE);
        FlowFile ffReturn = testRunner.getFlowFilesForRelationship(JsonToAttributeProcessor.FAILURE).get(0);
        assertThat(ffReturn).isNotNull();
    }

    @Test
    public void testProcessorNoJsonException() {
        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("[{'key':'value','maike':'will lead','object':{'sub':'type'}}]".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
        testRunner.run(1);
        testRunner.assertTransferCount(JsonToAttributeProcessor.FAILURE, 1);
        testRunner.assertAllFlowFilesTransferred(JsonToAttributeProcessor.FAILURE);
        FlowFile ffReturn = testRunner.getFlowFilesForRelationship(JsonToAttributeProcessor.FAILURE).get(0);
        assertThat(ffReturn).isNotNull();
    }

}
