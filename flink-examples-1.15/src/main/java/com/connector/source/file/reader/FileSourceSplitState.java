package com.connector.source.file.reader;

import com.connector.source.file.split.FileSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

public class FileSourceSplitState {
    private final FileSourceSplit split;
    private @Nullable Long offset;

    public FileSourceSplitState(FileSourceSplit split){
        this.split = split;
        this.offset = split.getReaderPosition().orElse(null);
    }

    public @Nullable Long getOffset() {
        return offset;
    }

    public void setOffset(@Nullable Long offset) {
        this.offset = offset;
    }

    public FileSourceSplit toFileSourceSplit(){
        final FileSourceSplit updateSplit = split.updateWithCheckpointedPosition(offset);

        if (updateSplit == null){
            throw new FlinkRuntimeException("");
        }
        if (updateSplit.getClass() !=split.getClass()){
            throw new FlinkRuntimeException("");
        }
        return updateSplit;
    }
}
