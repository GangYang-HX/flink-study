package com.connector.source.file.split;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Optional;

/**
 * 数据分片序列化器，对FileSourceSplit序列化和反序列化。数据分片在从SplitEnumerator传输到SourceReader，以及被SourceReader Checkpoint持久化时都需要序列化
 */
public class FileSourceSplitSerializer implements SimpleVersionedSerializer<FileSourceSplit> {

    public static final FileSourceSplitSerializer INSTANCE = new FileSourceSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZED_CACHE = ThreadLocal.withInitial(()->new DataOutputSerializer(64));

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FileSourceSplit split) throws IOException {
        Preconditions.checkArgument(split.getClass() == FileSourceSplit.class,"Cannot serialize subclasses of FileSourceSplit");
        if (split.serializedFormCache != null){
            return split.serializedFormCache;
        }
        final DataOutputSerializer out = SERIALIZED_CACHE.get();
        out.writeUTF(split.splitId());
        out.writeLong(split.offset());
        out.writeLong(split.length());

        split.path().write(out);

        final Optional<Long> readerPosition = split.getReaderPosition();
        out.writeBoolean(readerPosition.isPresent());

        if (readerPosition.isPresent()){
            out.writeLong(readerPosition.get());
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();

        split.serializedFormCache = result;

        return result;
    }

    @Override
    public FileSourceSplit deserialize(int version, byte[] bytes) throws IOException {
        if (version != VERSION){
            throw new IOException("Unknown version:"+version);
        }
        final DataInputDeserializer in =new DataInputDeserializer(bytes);
        final String id = in.readUTF();
        final Path path = new Path();
        path.read(in);
        final long offset = in.readLong();
        final long length = in.readLong();
        final Long readerPosition = in.readBoolean()?in.readLong():null;
        return new FileSourceSplit(id,path,offset,length,readerPosition,bytes);
    }
}
