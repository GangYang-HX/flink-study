package flink.examples.sql._03.source_sink.genrator_code;

public class LookupFunction$4
        extends org.apache.flink.api.common.functions.RichFlatMapFunction {

    private transient flink.examples.sql._03.source_sink.table.redis.v2.source.RedisRowDataLookupFunction function_flink$examples$sql$_03$source_sink$table$redis$v2$source$RedisRowDataLookupFunction$dad6ee99e28044bf601baf8c4ca75614;
    private TableFunctionResultConverterCollector$2 resultConverterCollector$3 = null;

    public LookupFunction$4(Object[] references) throws Exception {
        function_flink$examples$sql$_03$source_sink$table$redis$v2$source$RedisRowDataLookupFunction$dad6ee99e28044bf601baf8c4ca75614 = (((flink.examples.sql._03.source_sink.table.redis.v2.source.RedisRowDataLookupFunction) references[0]));
    }


    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {

        function_flink$examples$sql$_03$source_sink$table$redis$v2$source$RedisRowDataLookupFunction$dad6ee99e28044bf601baf8c4ca75614.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));


        resultConverterCollector$3 = new TableFunctionResultConverterCollector$2();
        resultConverterCollector$3.setRuntimeContext(getRuntimeContext());
        resultConverterCollector$3.open(new org.apache.flink.configuration.Configuration());


        function_flink$examples$sql$_03$source_sink$table$redis$v2$source$RedisRowDataLookupFunction$dad6ee99e28044bf601baf8c4ca75614.setCollector(resultConverterCollector$3);

    }

    @Override
    public void flatMap(Object _in1, org.apache.flink.util.Collector c) throws Exception {
        org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) _in1;

        org.apache.flink.table.data.binary.BinaryStringData field$0;
        boolean isNull$0;
        isNull$0 = in1.isNullAt(0);
        field$0 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        if (!isNull$0) {
            field$0 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(0));
        }

        resultConverterCollector$3.setCollector(c);


        if (isNull$0) {
            // skip
        } else {
            function_flink$examples$sql$_03$source_sink$table$redis$v2$source$RedisRowDataLookupFunction$dad6ee99e28044bf601baf8c4ca75614.eval(isNull$0 ? null : ((org.apache.flink.table.data.binary.BinaryStringData) field$0));
        }


    }

    @Override
    public void close() throws Exception {

        function_flink$examples$sql$_03$source_sink$table$redis$v2$source$RedisRowDataLookupFunction$dad6ee99e28044bf601baf8c4ca75614.close();

    }


    public class TableFunctionResultConverterCollector$2 extends org.apache.flink.table.runtime.collector.WrappingCollector {


        public TableFunctionResultConverterCollector$2() throws Exception {

        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {

        }

        @Override
        public void collect(Object record) {
            org.apache.flink.table.data.RowData externalResult$1 = (org.apache.flink.table.data.RowData) record;


            if (externalResult$1 != null) {
                outputResult(externalResult$1);
            }

        }

        @Override
        public void close() {
            try {

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}