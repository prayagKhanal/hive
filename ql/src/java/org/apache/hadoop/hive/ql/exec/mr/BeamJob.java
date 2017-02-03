package org.apache.hadoop.hive.ql.exec.mr;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hive.shims.CombineHiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes HadoopConf and run Hadoop MapReduce job as a Beam pipeline
 */
public class BeamJob {

  protected static final Logger LOG = LoggerFactory.getLogger(BeamJob.class);

  private interface Options extends PipelineOptions {
    // set temp jars, staging and temp work directories etc

  }

  private static Coder<KV<WritableComparable, Writable>> KV_CODER =
      KvCoder.of(new WritableCoder(CombineHiveKey.class), new WritableCoder(BytesWritable.class));

  public PipelineResult submit(JobConf jobConf) {

    PipelineOptions options = PipelineOptionsFactory
        .fromArgs(new String[0] /* XXX args */)
        .withValidation()
        .as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<WritableComparable, Writable>> elements = pipeline
        .apply(Read.from(new HiveInputFormatSource(jobConf)))
        .apply(ParDo.of(new MapperFn(jobConf)));

    if (jobConf.getNumReduceTasks() > 0) {
      elements = elements
          .apply(GroupByKey.<WritableComparable, Writable>create())
          .apply(ParDo.of(new ReducerFn(jobConf)));
    }

    // Hive does not use usual MR output format. But we might still have to invoke its
    // initialization.

    return pipeline.run();
  }

  private static class MapperFn extends
      DoFn<KV<WritableComparable, Writable>, KV<WritableComparable, Writable>> {

    private final SerializableWritable<JobConf> jobConfObj;
    private transient ExecMapper mapper;

    MapperFn(JobConf jobConf) {
      this.jobConfObj = new SerializableWritable<>(jobConf);
    }

    @Setup
    public void setup() throws IOException {
      checkArgument(jobConfObj.get().getMapperClass().equals(ExecMapper.class));
      mapper = ReflectionUtils.newInstance(ExecMapper.class, jobConfObj.get());
      mapper.configure(jobConfObj.get());
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws IOException {

      OutputCollector<Object, Object> outputCollector = new OutputCollector<Object, Object>() {
        @Override
        public void collect(Object key, Object value) throws IOException {
          c.output(KV.of((WritableComparable) key, (Writable) value));
        }
      };

      LOG.info("XXX : Got " + c.element().toString());

      mapper.map(c.element().getKey(), c.element().getValue(), outputCollector, null);
    }

    @Teardown
    public void tearDown() {
      mapper.close();
    }
  }

  private static class ReducerFn extends
      DoFn<KV<WritableComparable, Iterable<Writable>>, KV<WritableComparable, Writable>> {

    private final SerializableWritable<JobConf> jobConfObj;
    private transient ExecReducer reducer;

    ReducerFn(JobConf jobConf) {
      this.jobConfObj = new SerializableWritable<>(jobConf);
    }

    @Setup
    public void setup() throws IOException {
      checkArgument(jobConfObj.get().getMapperClass().equals(ExecMapper.class));
      reducer = ReflectionUtils.newInstance(ExecReducer.class, jobConfObj.get());
      reducer.configure(jobConfObj.get());
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws IOException {

      OutputCollector<Object, Object> outputCollector = new OutputCollector<Object, Object>() {
        @Override
        public void collect(Object key, Object value) throws IOException {
          c.output(KV.of((WritableComparable) key, (Writable) value));
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
      extends BoundedSource<KV<WritableComparable, Writable>> {

    final SerializableWritable<InputSplit> inputSplitObj;
    final SerializableWritable<JobConf> jobConfObj;

    public HiveInputFormatSource(JobConf jobConf, InputSplit inputSplit) {
      super();
      jobConfObj = new SerializableWritable<>(jobConf);
      inputSplitObj = new SerializableWritable<>(inputSplit);
    }

    public HiveInputFormatSource(JobConf jobConf) {
      super();
      jobConfObj = new SerializableWritable<>(jobConf);
      inputSplitObj = null;
    }

    @Override
    public Coder<KV<WritableComparable, Writable>> getDefaultOutputCoder() {
      return KV_CODER;
    }

    @Override
    public List<HiveInputFormatSource> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      InputFormat inputFormat = jobConfObj.get().getInputFormat();

      InputSplit[] inputSplits = inputFormat.getSplits(jobConfObj.get(), 1);

      List<HiveInputFormatSource> sources = new ArrayList<>(inputSplits.length);
      for (InputSplit split : inputSplits) {
        sources.add(new HiveInputFormatSource(jobConfObj.get(), split));
      }

      return sources;
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
      RecordReader<WritableComparable, Writable> reader =
          jobConfObj.get().getInputFormat().getRecordReader(
              inputSplitObj.get(), jobConfObj.get(), Reporter.NULL);
      return new HiveRecordReader(this, reader);
    }

    @Override
    public void validate() {
    }
  }

  public static class HiveRecordReader extends BoundedReader<KV<WritableComparable, Writable>> {

    private final HiveInputFormatSource source;
    private final RecordReader<WritableComparable, Writable> reader;
    private final WritableComparable key;
    private final Writable value;


    HiveRecordReader(HiveInputFormatSource source,
                     RecordReader<WritableComparable, Writable> reader) {
      this.source = source;
      this.reader = reader;
      key = reader.createKey();
      value = reader.createValue();
    }

    @Nullable
    @Override
    public BoundedSource<KV<WritableComparable, Writable>> splitAtFraction(double fraction) {
      return super.splitAtFraction(fraction);
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return super.getCurrentTimestamp();
    }

    @Override
    public BoundedSource<KV<WritableComparable, Writable>> getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      return reader.next(key, value);
    }

    @Override
    public KV<WritableComparable, Writable> getCurrent() throws NoSuchElementException {
      LOG.info("XXX : Got " + ((CombineHiveKey)key).getKey() + " - " + value + " - " + ((CombineHiveKey)key).getKey().getClass());
      return KV.of(key, value);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  private static class WritableCoder<W extends Writable> extends AtomicCoder<W> {

    private final Class<W> clazz;

    WritableCoder(Class<W> clazz) {
      this.clazz = clazz;
    }

    public void encode(W value, OutputStream outStream, Context context)
        throws IOException {
      value.write(new DataOutputStream(outStream));
    }

    @Override
    public W decode(InputStream inStream, Context context)
        throws IOException {
      try {
        W writable = (W) clazz.newInstance();
        writable.readFields(new DataInputStream(inStream));
        return writable;
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private static void readFields(Writable obj, byte[] bytes) throws IOException {
    DataInputBuffer input = new DataInputBuffer();
    input.reset(bytes, bytes.length);
    obj.readFields(input);
  }

  private static void readFieldsUnchecked(Writable obj, byte[] bytes) {
    try {
      readFields(obj, bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class SerializableWritable<W extends Writable> implements Serializable {
    private Class<? extends Writable> clazz;
    private transient W writable;

    SerializableWritable(W writable) {
      this.clazz = writable.getClass();
      this.writable = writable;
    }

    W get() {
      return writable;
    }

    private void writeObject(ObjectOutputStream oos)
        throws IOException {
      oos.defaultWriteObject();
      writable.write(new DataOutputStream(oos));
    }

    private void readObject(ObjectInputStream ois)
        throws ClassNotFoundException, IOException {
      ois.defaultReadObject();
      try {
        writable = (W) clazz.newInstance();
      } catch (Exception e) {
        throw new IOException(e);
      }
      writable.readFields(new DataInputStream(ois));
    }
  }
}
