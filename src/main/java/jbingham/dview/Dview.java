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

import java.util.Collection;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Create a viewer for a DAG in a dsub or other pipeline. A job provider
 * for the Google Genomics Pipelines API is used to check for job status.
 * <p>
 * The DAG must be explicitly defined in a YAML string containing job IDs or
 * job names. On Google Cloud, the job IDs are Google Genomics Operation IDs.
 * E.g.:
 * <pre>
 * - jobId1
 * - jobId2
 * - jobId3
 * </pre>
 * The special word "BRANCH" can be used to add branches to the graph:
 * <pre>
 * - jobName1
 * - BRANCH:
 *   - - jobName2
 *     - jobName3
 *   - jobName4
 * - jobName5
 * </pre>
 * </p>
 * <p>
 * In the future, it ought to be possible to lookup the whole DAG from the first job ID.
 * This would require that later operations store the job IDs of the jobs they're
 * dependent on.
 * </p>
 */
public class Dview {

  public interface DviewOptions extends PipelineOptions {
    @Description("Job IDs as a DAG definition in YAML. Either jobIDs or jobNames is required")
    String getJobIds();
    void setJobIds(String value);

    @Description("Job names as a DAG definition in YAML Either jobIDs or jobNames is required")
    String getJobNames();
    void setJobNames(String value);
  }
  
  @SuppressWarnings("serial")
  public static class MergeBranches extends PTransform<PCollectionList<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollectionList<String> input) {
      return input
          .apply(Flatten.<String>pCollections())
          .apply(Combine.globally(new SerializableFunction<Iterable<String>,String>() {
              public String apply(Iterable<String> input) {
                if (input != null && input.iterator().hasNext()) {
                  return "merge";
                } else {
                  return null;
                }
              }
            }));    
    }
  }

  @SuppressWarnings("serial")
  public static class WaitForJob extends PTransform<PCollection<String>, PCollection<String>> {
    String projectId, jobId, jobName;
    
    public WaitForJob(String projectId, String jobId, String jobName) {
      this.projectId = projectId;
      this.jobId = jobId;
      this.jobName = jobName;
    }

    public PCollection<String> expand(PCollection<String> input) {
      return input.apply("BreakFusion", ParDo.of(new DoFn<String, KV<String,String>>(){
              @ProcessElement
              public void processElement(DoFn<String, KV<String, String>>.ProcessContext c) 
                  throws Exception {
               c.output(KV.of(String.valueOf(c.element().hashCode()), c.element()));
              }
            }))
          .apply(Combine.<String, String>perKey(new SerializableFunction<Iterable<String>,String>(){
              public String apply(Iterable<String> input) {
                return input.iterator() != null && input.iterator().hasNext()
                    ? input.iterator().next()
                    : null;
              }
            }))
          .apply(Values.<String>create())
          .apply("Wait", ParDo.of(new DoFn<String,String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                LOG.debug("Input: " + c.element());
                if (jobId == null) {
                  LOG.info("Looking up ID for job named " + jobName);
                  jobId = provider.getJobId(projectId, jobName);
                }
                provider.waitForJob(jobId);
                c.output(jobId);
              }             
            }));
    }
  }

  static final Logger LOG = LoggerFactory.getLogger(Dview.class);
  private static GooglePipelinesProvider provider = new GooglePipelinesProvider();
  private static boolean convertJobNamesToIds;
  private static String projectId;

  /**
   * Recursively create the subgraph for each branch in a branching pipeline.
   *
   * @param branches
   * @param input inputs to the branches
   * @return root node in the subgraph
   */
  private PCollection<String> createBranches(Collection<?> branches, PCollection<String> input) {  
    LOG.info("Branch count: " + branches.size());
    PCollectionList<String> list = null;

    for (Object branch : branches) {
      LOG.info("Adding branch");

      // Recursively create a graph for each branch
      PCollection<String> branchOutput = createGraph(branch, input);
      list = list == null ? PCollectionList.of(branchOutput) : list.and(branchOutput);
    }
    
    LOG.info("Merging " + list.size() + " branches");
    return list.apply("MergeBranches", new MergeBranches());
  }

  /**
   * Recursively construct the Beam pipeline graph.
   *
   * @param graphItem a single task, list of tasks, or branch point
   * @param input inputs to the graph item
   * @return root node in the (sub)graph
   */
   private PCollection<String> createGraph(Object graphItem, PCollection<String> input) {
    PCollection<String> output = null;

    // A single task
    if (graphItem instanceof String) {
      LOG.info("Adding job: " + graphItem);
      
      String jobId = null;
      String jobName;

      if (convertJobNamesToIds) {
        jobName = (String)graphItem;
      } else {
        jobId = (String)graphItem;
        jobName = provider.getJobName(jobId);
      }
      LOG.info("Job name: " + jobName);

      output = input.apply(jobName, new WaitForJob(projectId, jobId, jobName));

    // A list of tasks
    } else if (graphItem instanceof Collection<?>) {
      output = input;

      // The output of each task is input to the next
      Collection<?> tasks = (Collection<?>)graphItem;
      for (Object task : tasks) {
        output = createGraph(task, output);
      }

    // A branch in the graph
    } else if (graphItem instanceof Map<?,?>) {
      LOG.info("Adding branches");

      Collection<?> branches =
          (Collection<?>)((Map<?,?>) graphItem).values().iterator().next();
      output = createBranches(branches, input);

    } else {
      throw new IllegalStateException("Invalid graph item: " + graphItem);
    }
    return output;
  }

  private void createAndRunPipeline(String[] args) {
    PipelineOptionsFactory.register(DviewOptions.class);
    DviewOptions options = 
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DviewOptions.class);

    if (options.getJobIds() == null && options.getJobNames() == null) {
      throw new IllegalArgumentException("Either jobIds or jobNames is required");
    }
    if (options.getJobIds() != null && options.getJobNames() != null) {
      throw new IllegalArgumentException("Only one of jobIds and jobNames can be set");
    }

    Yaml yaml = new Yaml();
    Object graph;
    if (options.getJobIds() != null) {
      graph = yaml.load(options.getJobIds());
    } else {
      convertJobNamesToIds = true;
      graph = yaml.load(options.getJobNames());
    }
    LOG.info("Graph: \n" + yaml.dump(graph));

    DataflowPipelineOptions dpo = options.as(DataflowPipelineOptions.class);
    projectId = dpo.getProject();

    Pipeline p = Pipeline.create(dpo);
    PCollection<String> input = p.apply("StartPipeline", Create.of("Pipeline"));
    createGraph(graph, input);

    p.run().waitUntilFinish();
    LOG.info("Dview job completed");
  }

  public static void main(String[] args) {
    new Dview().createAndRunPipeline(args);
  }
}