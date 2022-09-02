package com.connector.source.file.enumerator;

import com.connector.source.file.source.FileSource;
import com.connector.source.file.split.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

/**
 * SplitEnumerator checkpoint序列化器，对PendingSplitsCheckpoint进行序列化和发序列化
 */
public class PendingSplitsCheckpointSerializer implements SimpleVersionedSerializer<PendingSplitsCheckpoint> {

    private static final int VERSION = 1;

    private static final int VERSION_1_MAGIC_NUMBER = 0xDEADBEEF;

    private final SimpleVersionedSerializer<FileSourceSplit> splitSerializer;

    public PendingSplitsCheckpointSerializer(SimpleVersionedSerializer<FileSourceSplit> splitSerializer) {
        this.splitSerializer = Preconditions.checkNotNull(splitSerializer);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PendingSplitsCheckpoint checkpoint) throws IOException {
        Preconditions.checkArgument(checkpoint.getClass() == PendingSplitsCheckpoint.class, "Cannot serialize subclass of PendingSplitsCheckpoint");
        if (checkpoint.serializedFormCache != null) {
            return checkpoint.serializedFormCache;
        }
        final SimpleVersionedSerializer<FileSourceSplit> splitSerializer = this.splitSerializer;
        final Collection<FileSourceSplit> splits = checkpoint.getSplits();
        final Collection<Path> processedPaths = checkpoint.getAlreadyProcessedPaths();

        final ArrayList<byte[]> serializerSplits = new ArrayList<>(splits.size());
        final ArrayList<byte[]> serializerPaths = new ArrayList<>(processedPaths.size());

        int totalLen = 16;

        for (FileSourceSplit split : splits) {
            final byte[] serSplit = splitSerializer.serialize(split);
            serializerSplits.add(serSplit);
            totalLen += serSplit.length + 4;
        }

        for (Path path : processedPaths) {
            final byte[] serPath = path.toString().getBytes(StandardCharsets.UTF_8);
            serializerPaths.add(serPath);
            totalLen += serPath.length + 4;
        }

        final byte[] result = new byte[totalLen];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(VERSION_1_MAGIC_NUMBER);
        byteBuffer.putInt(splitSerializer.getVersion());
        byteBuffer.putInt(serializerSplits.size());
        byteBuffer.putInt(serializerPaths.size());

        for (byte[] splitBytes : serializerSplits) {
            byteBuffer.putInt(splitBytes.length);
            byteBuffer.put(splitBytes);
        }

        for (byte[] pathBytes : serializerPaths) {
            byteBuffer.putInt(pathBytes.length);
            byteBuffer.put(pathBytes);
        }
        assert byteBuffer.remaining() == 0;
        checkpoint.serializedFormCache = result;
        return result;
    }

    @Override
    public PendingSplitsCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version:" + version);
        }
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);
        final int magic = bb.getInt();
        if (magic != VERSION_1_MAGIC_NUMBER) {
            throw new IOException("");
        }
        final int splitSerializerVersion = bb.getInt();
        final int numSplits = bb.getInt();
        final int numPaths = bb.getInt();

        final SimpleVersionedSerializer<FileSourceSplit> splitSerializer = this.splitSerializer;
        final ArrayList<FileSourceSplit> splits = new ArrayList<>(numSplits);
        final ArrayList<Path> paths = new ArrayList<>(numPaths);

        for (int remaining = numSplits;remaining>0;remaining--){
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final FileSourceSplit split = splitSerializer.deserialize(splitSerializerVersion,bytes);
            splits.add(split);
        }

        for (int remaining=numPaths;remaining>0;remaining--){
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final Path path = new Path(new String(bytes,StandardCharsets.UTF_8));
            paths.add(path);
        }
        return new PendingSplitsCheckpoint(splits,paths);
    }
}
