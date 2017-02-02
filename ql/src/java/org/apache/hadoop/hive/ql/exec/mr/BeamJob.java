package org.apache.hadoop.hive.ql.exec.mr;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.Instant;

/**
 * Takes HadoopConf and run Hadoop MapReduce job as a Beam pipeline
 */
public class BeamJob {

  private interface Options extends PipelineOptions {
    // set temp jars, staging and temp work directories etc

  }

  public PipelineResult submit(JobConf jobConf) {

    PipelineOptions options = PipelineOptionsFactory
        .fromArgs(new String[0] /* XXX args */)
        .withValidation()
        .as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<BytesWritable, BytesWritable>> elements = pipeline
        .apply(Read.from(new HiveInputFormatSource(jobConf)))
        .apply(ParDo.of(new MapperFn(jobConf)));

    if (jobConf.getNumReduceTasks() > 0) {
      elements = elements
          .apply(GroupByKey.<BytesWritable, BytesWritable>create())
          .apply(ParDo.of(new ReducerFn(jobConf)));
    }

    // Hive does not use usual MR output format. But we might still have to invoke its
    // initialization.

    return pipeline.run();
  }

  private static class MapperFn extends
      DoFn<KV<BytesWritable, BytesWritable>, KV<BytesWritable, BytesWritable>> {

    private final JobConf jobConf;
    private transient ExecMapper mapper;

    MapperFn(JobConf jobConf) {
      this.jobConf = jobConf;
    }

    @Setup
    public void setup() throws IOException {
      checkArgument(jobConf.getMapperClass().equals(ExecMapper.class));
      mapper = ReflectionUtils.newInstance(ExecMapper.class, jobConf);
      mapper.configure(jobConf);
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws IOException {

      // XXX What should be the type?
      OutputCollector<Object, Object> outputCollector = new OutputCollector<Object, Object>() {
        @Override
        public void collect(Object key, Object value) throws IOException {
          c.output(KV.of((BytesWritable) key, (BytesWritable) value));
        }
      };

      mapper.map(c.element().getKey(), c.element().getValue(), outputCollector, null);
    }

    @Teardown
    public void tearDown() {
      mapper.close();
    }
  }

  private static class ReducerFn extends
      DoFn<KV<BytesWritable, Iterable<BytesWritable>>, KV<BytesWritable, BytesWritable>> {

    private final JobConf jobConf;
    private transient ExecReducer reducer;

    ReducerFn(JobConf jobConf) {
      this.jobConf = jobConf;
    }

    @Setup
    public void setup() throws IOException {
      checkArgument(jobConf.getMapperClass().equals(ExecMapper.class));
      reducer = ReflectionUtils.newInstance(ExecReducer.class, jobConf);
      reducer.configure(jobConf);
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws IOException {

      // XXX What should be the type?
      OutputCollector<Object, Object> outputCollector = new OutputCollector<Object, Object>() {
        @Override
        public void collect(Object key, Object value) throws IOException {
          c.output(KV.of((BytesWritable) key, (BytesWritable) value));
        }
      };

      reducer.reduce(
          c.element().getKey(), c.element().getValue().iterator(), outputCollector, null);
    }

    @Teardown
    public void tearDown() {
      reducer.close();
    }
  }

  /**
   * Beam read and write transforms for Hadoop InputFormat and and Hadoop OutputFormat.
   */
  public static class HiveInputFormatSource
      extends BoundedSource<KV<BytesWritable, BytesWritable>> {

    Configuration conf;

    public HiveInputFormatSource(Configuration conf) {
      super();
      this.conf = conf;
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
    }

    @Override
    public List<HiveInputFormatSource> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return null; // XXX
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public HiveRecordReader createReader(PipelineOptions options) throws IOException {
      return null;
    }

    @Override
    public void validate() {

    }

    @Override
    public Coder<> getDefaultOutputCoder() {
      return null;
    }
  }

  public static class HiveRecordReader extends BoundedReader<KV<BytesWritable, BytesWritable>> {

    private HiveInputFormatSource source;
    private RecordReader<BytesWritable, BytesWritable> hiveReader;
    private BytesWritable key;
    private BytesWritable value;

    @Nullable
    @Override
    public Double getFractionConsumed() {
      return super.getFractionConsumed();
    }

    @Override
    public long getSplitPointsConsumed() {
      return super.getSplitPointsConsumed();
    }

    @Override
    public long getSplitPointsRemaining() {
      return super.getSplitPointsRemaining();
    }

    @Nullable
    @Override
    public BoundedSource<KV<BytesWritable, BytesWritable>> splitAtFraction(double fraction) {
      return super.splitAtFraction(fraction);
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return super.getCurrentTimestamp();
    }

    @Override
    public BoundedSource<KV<BytesWritable, BytesWritable>> getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      return hiveReader.next(key, value);
    }

    @Override
    public KV<BytesWritable, BytesWritable> getCurrent() throws NoSuchElementException {
      return KV.of(key, value);
    }

    @Override
    public void close() throws IOException {
      hiveReader.close();
    }
  }

}
