/*
 * Copyright 2017 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jbingham.dview;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.genomics.model.DockerExecutor;
import com.google.api.services.genomics.model.LoggingOptions;
import com.google.api.services.genomics.model.Operation;
import com.google.api.services.genomics.model.Pipeline;
import com.google.api.services.genomics.model.PipelineResources;
import com.google.api.services.genomics.model.RunPipelineArgs;

import junit.framework.TestCase;

/**
 * Integration test. To run, set environment variables TEST_PROJECT and TEST_GCS_PATH.
 * Also, enable the storage, compute, genomics, and dataflow APIs.
 */
public class DviewIT extends TestCase {
  static final Logger LOG = LoggerFactory.getLogger(DviewIT.class);
  private static final String TEST_PROJECT = System.getenv("TEST_PROJECT");
  private static final String TEST_GCS_PATH = System.getenv("TEST_GCS_PATH");
  private static final String TEST_ZONES = "us-central1-a";

  public DviewIT() {
    assertNotNull("You must set the TEST_PROJECT environment variable.", TEST_PROJECT);
    assertNotNull("You must set the TEST_GCS_PATH environment variable.", TEST_GCS_PATH);
    assertTrue("TEST_GCS_PATH must begin with gs:// ", TEST_GCS_PATH.startsWith("gs://"));
    assertTrue(
        "TEST_GCS_PATH must not end with a trailing slash /",
        !TEST_GCS_PATH.endsWith("/"));

    LOG.info("TEST_PROJECT=" + TEST_PROJECT);
    LOG.info("TEST_GCS_PATH=" + TEST_GCS_PATH);
  }

  @Test
  public void testDviewJobIdsDirect() throws IOException, GeneralSecurityException {
    String yaml = createJobIdYaml();

    Dview.main(new String[] { 
        "--project=" + TEST_PROJECT,
        "--tempLocation=" + TEST_GCS_PATH,
        "--runner=direct",
        "--jobIds=" + yaml
    });
  }
  
  @Test
  public void testDviewJobIdsDataflow() throws IOException, GeneralSecurityException {
    String yaml = createJobIdYaml();

    Dview.main(new String[] { 
        "--project=" + TEST_PROJECT,
        "--tempLocation=" + TEST_GCS_PATH,
        "--runner=dataflow",
        "--jobIds=" + yaml
    });
  }

  @Test
  public void testDviewJobNamesDirect() throws IOException, GeneralSecurityException {
    String yaml = createJobNameYaml();

    Dview.main(new String[] { 
        "--project=" + TEST_PROJECT,
        "--tempLocation=" + TEST_GCS_PATH,
        "--runner=direct",
        "--jobNames=" + yaml
    });
  }
  
  @Test
  public void testDviewJobNamesDataflow() throws IOException, GeneralSecurityException {
    String yaml = createJobNameYaml();

    Dview.main(new String[] { 
        "--project=" + TEST_PROJECT,
        "--tempLocation=" + TEST_GCS_PATH,
        "--runner=dataflow",
        "--jobNames=" + yaml
    });
  }
  
  private String createJobIdYaml() throws IOException, GeneralSecurityException {
    // Submit jobs to get job IDs. Add delays to simulate task dependency.
    String jobId1 = submitJob("job1", 0).getName();
    String jobId2a = submitJob("job2a", 60 * 2).getName();
    String jobId2b = submitJob("job2b", 60 * 2).getName();
    String jobId3 = submitJob("job3", 60 * 4).getName();
    String jobId4 = submitJob("job4", 60 * 6).getName();
    
    String yaml = 
        "- " + jobId1 + "\n\n" +
        "- BRANCH:\n\n" +
        "  - - " + jobId2a + "\n\n" +
        "    - " + jobId3 + "\n\n" +
        "  - " + jobId2b + "\n\n" +
        "- " + jobId4;    
    return yaml;
  }
  
  private String createJobNameYaml() throws IOException, GeneralSecurityException {
    // Submit jobs to get job IDs. Add delays to simulate task dependency.
    submitJob("jobN1", 60 * 2).getName();
    submitJob("jobN2a", 60 * 4).getName();
    submitJob("jobN2b", 60 * 4).getName();
    submitJob("jobN3", 60 * 6).getName();
    submitJob("jobN4", 60 * 8).getName();
    
    String yaml = 
        "- jobN1\n\n" +
        "- BRANCH:\n\n" +
        "  - - jobN2a\n\n" +
        "    - jobN3\n\n" +
        "  - jobN2b\n\n" +
        "- jobN4";    
    return yaml;
  }

  private Operation submitJob(String jobName, int sleepTime) throws IOException, GeneralSecurityException {
    PipelineResources resources = new PipelineResources();
    resources.setZones(Collections.singletonList(TEST_ZONES));

    DockerExecutor docker = new DockerExecutor();
    docker.setImageName("ubuntu");
    docker.setCmd("echo hello; sleep " + sleepTime);

    Pipeline pipeline = new Pipeline();
    pipeline.setProjectId(TEST_PROJECT);
    pipeline.setName(jobName);
    pipeline.setResources(resources);
    pipeline.setDocker(docker);

    LoggingOptions logging = new LoggingOptions();
    logging.setGcsPath(TEST_GCS_PATH);
    
    Map<String,String> labels = new HashMap<String,String>();
    labels.put("job-name", jobName.toLowerCase());

    RunPipelineArgs args = new RunPipelineArgs();
    args.setProjectId(TEST_PROJECT);
    args.setLogging(logging);
    args.setLabels(labels);
    
    GooglePipelinesProvider provider = new GooglePipelinesProvider();
    Operation operation = provider.submitJob(pipeline, args);
    return operation;    
  }
}