package com.connector.source.file.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

/**
 * 实现SourceSplit。
 * 该类保存了数据分片id、文件路径、数据分片起始位置的文件偏移（我们这里整个文件作为一个数据分片，不再细分，因此偏移始终为0）、文件长度、文件读取进度（恢复时从该位置继续数据读取）
 */
public class FileSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String id; //分片ID
    private final Path filePath; //文件路径
    private final long offset; //分片起始位置
    private final long length;//文件长度

    @Nullable private final Long readerPosition; //文件读取进度
    @Nullable transient byte[] serializedFormCache;

    public FileSourceSplit(String id,Path filePath,long offset,long length){
        this(id,filePath,offset,length,null);
    }

    public FileSourceSplit(String id,Path filePath,long offset,long length ,@Nullable Long readerPosition){
        this(id,filePath,offset,length,readerPosition,null);
    }

    public FileSourceSplit(String id,Path filePath,long offset,long length ,@Nullable Long readerPosition,@Nullable  byte[] serializedForm){
        this.id = Preconditions.checkNotNull(id);
        this.filePath = Preconditions.checkNotNull(filePath);
        this.offset = offset;
        this.length = length;
        this.readerPosition = readerPosition;
        this.serializedFormCache = serializedForm;
    }

    @Override
    public String splitId() {
        return id;
    }

    public Path path() {
        return filePath;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public Optional<Long> getReaderPosition(){
        return Optional.ofNullable(readerPosition);
    }

    public FileSourceSplit updateWithCheckpointedPosition(@Nullable Long position){
        return new FileSourceSplit(id,filePath,offset,length,position);
    }

    @Override
    public String toString() {
        return "FileSourceSplit{"
                + "id='"
                + id
                + '\''
                + ", filePath="
                + filePath
                + ", offset="
                + offset
                + ", length="
                + length
                + ", readerPosition="
                + readerPosition
                + '}';
    }


}
