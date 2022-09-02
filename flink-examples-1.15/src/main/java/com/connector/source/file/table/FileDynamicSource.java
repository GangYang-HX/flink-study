package com.connector.source.file.table;

import com.connector.source.file.source.FileSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.stream.Stream;

public class FileDynamicSource implements ScanTableSource {

    private final Path[] paths;
    private final ResolvedSchema schema;

    public FileDynamicSource(String path,ResolvedSchema schema){
        this(Stream.of(Preconditions.checkNotNull(path)).map(Path::new).toArray(Path[]::new),schema);
    }

    public FileDynamicSource(Path[] paths,ResolvedSchema schema){
        this.paths = Preconditions.checkNotNull(paths);
        this.schema = Preconditions.checkNotNull(schema);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final TypeInformation<RowData> producedTypeInfo = scanContext.createTypeInformation(schema.toPhysicalRowDataType());
        return SourceProvider.of(new FileSource(producedTypeInfo,paths));
    }

    @Override
    public DynamicTableSource copy() {
        return new FileDynamicSource(paths,schema);
    }

    @Override
    public String asSummaryString() {
        return "FileSource";
    }
}
