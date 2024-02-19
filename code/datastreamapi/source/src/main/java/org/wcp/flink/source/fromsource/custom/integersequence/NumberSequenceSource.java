package org.wcp.flink.source.fromsource.custom.integersequence;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.*;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Public
public class NumberSequenceSource implements Source<Long, NumberSequenceSource.NumberSequenceSplit, Collection<NumberSequenceSource.NumberSequenceSplit>>, ResultTypeQueryable<Long> {
//    private static final long serialVersionUID = 1L;
    private final long from;
    private final long to;

    public NumberSequenceSource(long from, long to) {
        Preconditions.checkArgument(from <= to, "'from' must be <= 'to'");
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return this.from;
    }

    public long getTo() {
        return this.to;
    }

    @Override
    public TypeInformation<Long> getProducedType() {
        return Types.LONG;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Long, NumberSequenceSplit> createReader(SourceReaderContext readerContext) {
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> createEnumerator(SplitEnumeratorContext<NumberSequenceSplit> enumContext) {
        List<NumberSequenceSplit> splits = this.splitNumberRange(this.from, this.to, enumContext.currentParallelism());
        return new IteratorSourceEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> restoreEnumerator(SplitEnumeratorContext<NumberSequenceSplit> enumContext, Collection<NumberSequenceSplit> checkpoint) {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NumberSequenceSplit> getSplitSerializer() {
        return new SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NumberSequenceSplit>> getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer();
    }

    protected List<NumberSequenceSource.NumberSequenceSplit> splitNumberRange(long from, long to, int numSplits) {
        NumberSequenceIterator[] subSequences = (new NumberSequenceIterator(from, to)).split(numSplits);
        ArrayList<NumberSequenceSplit> splits = new ArrayList<>(subSequences.length);
        int splitId = 1;
        NumberSequenceIterator[] var9 = subSequences;
        int var10 = subSequences.length;

        for(int var11 = 0; var11 < var10; ++var11) {
            NumberSequenceIterator seq = var9[var11];
            if (seq.hasNext()) {
                splits.add(new NumberSequenceSplit(String.valueOf(splitId++), seq.getCurrent(), seq.getTo()));
            }
        }
        return splits;
    }

    private static final class CheckpointSerializer implements SimpleVersionedSerializer<Collection<NumberSequenceSplit>> {
        private static final int CURRENT_VERSION = 1;

        private CheckpointSerializer() {
        }

        public int getVersion() {
            return 1;
        }

        public byte[] serialize(Collection<NumberSequenceSplit> checkpoint) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(checkpoint.size() * 22 + 4);
            out.writeInt(checkpoint.size());

            for (NumberSequenceSplit split : checkpoint) {
                SplitSerializer.serializeV1(out, split);
            }

            return out.getCopyOfBuffer();
        }

        public Collection<NumberSequenceSplit> deserialize(int version, byte[] serialized) throws IOException {
            if (version != 1) {
                throw new IOException("Unrecognized version: " + version);
            } else {
                DataInputDeserializer in = new DataInputDeserializer(serialized);
                int num = in.readInt();
                ArrayList<NumberSequenceSplit> result = new ArrayList<>(num);

                for(int remaining = num; remaining > 0; --remaining) {
                    result.add(SplitSerializer.deserializeV1(in));
                }

                return result;
            }
        }
    }

    private static final class SplitSerializer implements SimpleVersionedSerializer<NumberSequenceSplit> {
        private static final int CURRENT_VERSION = 1;

        private SplitSerializer() {
        }

        public int getVersion() {
            return 1;
        }

        public byte[] serialize(NumberSequenceSplit split) throws IOException {
            Preconditions.checkArgument(split.getClass() == NumberSequenceSplit.class, "cannot serialize subclasses");
            DataOutputSerializer out = new DataOutputSerializer(split.splitId().length() + 18);
            serializeV1(out, split);
            return out.getCopyOfBuffer();
        }

        public NumberSequenceSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version != 1) {
                throw new IOException("Unrecognized version: " + version);
            } else {
                DataInputDeserializer in = new DataInputDeserializer(serialized);
                return deserializeV1(in);
            }
        }

        static void serializeV1(DataOutputView out, NumberSequenceSplit split) throws IOException {
            out.writeUTF(split.splitId());
            out.writeLong(split.from());
            out.writeLong(split.to());
        }

        static NumberSequenceSplit deserializeV1(DataInputView in) throws IOException {
            return new NumberSequenceSource.NumberSequenceSplit(in.readUTF(), in.readLong(), in.readLong());
        }
    }

    public static class NumberSequenceSplit implements IteratorSourceSplit<Long, NumberSequenceIterator> {
        private final String splitId;
        private final long from;
        private final long to;

        public NumberSequenceSplit(String splitId, long from, long to) {
            Preconditions.checkArgument(from <= to, "'from' must be <= 'to'");
            this.splitId = (String)Preconditions.checkNotNull(splitId);
            this.from = from;
            this.to = to;
        }

        public String splitId() {
            return this.splitId;
        }

        public long from() {
            return this.from;
        }

        public long to() {
            return this.to;
        }

        public NumberSequenceIterator getIterator() {
            return new NumberSequenceIterator(this.from, this.to);
        }

        public IteratorSourceSplit<Long, NumberSequenceIterator> getUpdatedSplitForIterator(NumberSequenceIterator iterator) {
            return new NumberSequenceSplit(this.splitId, iterator.getCurrent(), iterator.getTo());
        }

        public String toString() {
            return String.format("NumberSequenceSplit [%d, %d] (%s)", this.from, this.to, this.splitId);
        }
    }
}
