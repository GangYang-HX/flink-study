package com.connector.source.file.reader;

import com.connector.source.file.split.FileSourceSplit;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.table.data.RowData;

import java.util.Map;

/**
 * 继承自SingleThreadMultiplexSourceReaderBase，在读取完一个数据分片（文件）后再向FileSourceEnumerator请求下一个分片。
 * 我们需要实现数据分片状态初始化initializedState，当新的数据分片加入时会调用该接口。
 * 实现接口toSplitType把可变的数据分片状态FileSourceSplitState转换为不可变的数据分片FileSourceSplit,checkpoint时会调用该接口得到最新状态的FileSourceSplit并持久化。
 * FileSourceRecordEmitter发送数据到下游，并更新FileSourceSplitState的分片读取进度。具体分片数据读取逻辑在FileSourceSplitReader实现，这里我们简单的每次读取一行数据
 */
public class FileSourceReader extends SingleThreadMultiplexSourceReaderBase<RecordAndPosition, RowData, FileSourceSplit,FileSourceSplitState> {

    public FileSourceReader(SourceReaderContext readerContext){
        super(FileSourceSplitReader::new,new FileSourceReaderEmitter(),readerContext.getConfiguration(),readerContext);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() ==0){
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, FileSourceSplitState> map) {
        context.sendSplitRequest();
    }

    @Override
    protected FileSourceSplitState initializedState(FileSourceSplit split) {
        return new FileSourceSplitState(split);
    }

    @Override
    protected FileSourceSplit toSplitType(String splitId, FileSourceSplitState fileSourceSplitState) {
        return fileSourceSplitState.toFileSourceSplit();
    }
}
