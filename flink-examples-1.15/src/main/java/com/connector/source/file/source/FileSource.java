package com.connector.source.file.source;

import com.connector.source.file.enumerator.FileSourceEnumerator;
import com.connector.source.file.enumerator.PendingSplitsCheckpoint;
import com.connector.source.file.enumerator.PendingSplitsCheckpointSerializer;
import com.connector.source.file.reader.FileSourceReader;
import com.connector.source.file.split.FileSourceSplit;
import com.connector.source.file.split.FileSourceSplitSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Collections;

/**
 * FileSource是一个工厂类，用于创建SplitEnumerator、SourceReader、数据分片序列化器、SplitEnumerator Checkpoint序列化器。
 * 实现source接口，需要三个类型参数：第一个类型参数为Source输出数据类型；第二个类型参数为数据分片类型SourceSplit；第三个类型参数为SplitEnumerator Checkpoint类型；
 */
public class FileSource implements Source<RowData, FileSourceSplit, PendingSplitsCheckpoint>, ResultTypeQueryable<RowData> {

    private final TypeInformation<RowData> producedTypeInfo;
    private final Path[] paths;

    public FileSource(TypeInformation<RowData> producedTypeInfo,Path[] paths){
        this.producedTypeInfo = Preconditions.checkNotNull(producedTypeInfo);
        this.paths = Preconditions.checkNotNull(paths);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, FileSourceSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        return new FileSourceReader(sourceReaderContext);
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> createEnumerator(SplitEnumeratorContext<FileSourceSplit> splitEnumeratorContext) throws Exception {
        return new FileSourceEnumerator(splitEnumeratorContext,paths, Collections.emptyList(),Collections.emptyList());
    }

    // 恢复SplitEnumerator，比如任务故障重启，会根据不同的checkpoint恢复对应的SplitEnumerator，用于继续之前未完成的读取操作
    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(SplitEnumeratorContext<FileSourceSplit> splitEnumeratorContext, PendingSplitsCheckpoint pendingSplitsCheckpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return FileSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
