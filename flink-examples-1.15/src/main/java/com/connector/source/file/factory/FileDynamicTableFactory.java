package com.connector.source.file.factory;

import com.connector.source.file.table.FileDynamicSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FileDynamicTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "file";

    private static final ConfigOption<String> PATH = ConfigOptions.key("path").stringType().noDefaultValue().withDescription("The path of a directory");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this,context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();
        final ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        final List<Column> physicalColumns = schema.getColumns().stream().filter(Column::isPhysical).collect(Collectors.toList());

        if (physicalColumns.size() !=1 || !physicalColumns.get(0).getDataType().getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)){
            throw new ValidationException("");
        }
        return new FileDynamicSource(config.get(PATH),schema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
