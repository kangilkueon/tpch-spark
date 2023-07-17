package csdvirt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.json4s.jackson.JsonMethods$;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import scala.collection.JavaConverters;
import org.apache.spark.unsafe.types.UTF8String;
import java.util.function.Function;

import tpch.TpchSchemaProviderTemp$;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

public class DefaultSource  implements TableProvider {
    public static String getTableName(String path) {
        System.out.println("==================== CSDVIRT GetTableNAme ====================");
        String[] names = path.split("/");
        System.out.println("====================" + names[names.length - 1] + "====================");
        return names[names.length - 1];
    }

    public StructType inferSchema(CaseInsensitiveStringMap options) {
        System.out.println("==================== CSDVIRT inferSchema ====================");
        StructType schema = TpchSchemaProviderTemp$.MODULE$.getSchema(getTableName(options.get("path")));
        StructField[] fields = schema.fields();

        System.out.println("==================== CSDVIRT inferSchema ====================" + fields.length);
        return schema;
    }

    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        System.out.println("==================== CSDVIRT getTable ====================");
        return new CSDVirtBatchTable(schema, properties);
    }
}

class CSDVirtBatchTable implements SupportsRead { //, Table {
    private final StructType schema_;
    private final Map<String, String> properties_;
    private final String path_;

    public CSDVirtBatchTable(StructType schema, Map<String, String> properties) {
        System.out.println("==================== CSDVIRT CSDVirtBatchTable ====================");
        schema_ = schema;
        properties_ = properties;
        path_ = properties.get("path");
        System.out.println("==================== CSDVIRT CSDVirtBatchTable ====================" + path_);
    }

    public String name() {
        System.out.println("==================== CSDVIRT name ====================");
        return DefaultSource.getTableName(path_) + "@" + path_;
    }

    public StructType schema() {
        System.out.println("==================== CSDVIRT schema ====================");
        return schema_;
    }

    public Set<TableCapability> capabilities() {
        System.out.println("==================== CSDVIRT capabilities ====================");
        return new HashSet<>(Collections.singletonList(TableCapability.BATCH_READ));
    }

    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        System.out.println("==================== CSDVIRT newScanBuilder ====================");
        return new CSDVirtScanBuilder(schema_, properties_, options);
    }
}

class CSDVirtScanBuilder implements ScanBuilder { //, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
    public static final List<Class<? extends Filter>> unsupportedFilters = Collections.unmodifiableList(Arrays.asList(IsNull.class, IsNotNull.class, EqualNullSafe.class, And.class, In.class, AlwaysTrue.class, AlwaysFalse.class));
    private final StructType schema_;
    private final Map<String, String> properties_;
    private final CaseInsensitiveStringMap options_;
    private StructType requiredSchema_;
    // private final String path_;

    private Filter[] pushedFilters_;

    public CSDVirtScanBuilder(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        System.out.println("==================== CSDVIRT CSDVirtScanBuilder ====================");
        schema_ = schema;
        // path_ = path;
        properties_ = properties;
        options_ = options;

        pushedFilters_ = new Filter[0];
    }

    @Override
    public Scan build() {
        System.out.println("==================== CSDVIRT build ====================");
        // return null;
        return new CSDVirtScan(schema_, properties_, options_);
    }
}

class CSDVirtScan implements Scan {
    private final StructType readSchema_;
    private final Map<String, String> properties_;
    private final CaseInsensitiveStringMap options_;

    public CSDVirtScan(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        System.out.println("==================== CSDVIRT CSDVirtBatchScan ====================");
        readSchema_ = schema;
        properties_ = properties;
        options_ = options;
    }

    @Override
    public StructType readSchema() {
        System.out.println("==================== CSDVIRT readSchema ====================");
        return readSchema_;
    }

    @Override
    public Batch toBatch() {
        System.out.println("==================== CSDVIRT toBatch ====================");
        return new CSDVirtBatch(readSchema_, properties_, options_);
    }
}

class CSDVirtBatch implements Batch {
    private final StructType readSchema_;    
    private final Map<String, String> properties_;
    private final CaseInsensitiveStringMap options_;
    private final String filename;

    public CSDVirtBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {

        System.out.println("==================== CSDVIRT CSDVirtBatch ====================");
        readSchema_ = schema;
        properties_ = properties;
        options_ = options;
        this.filename = properties.get("path"); //options.get("fileName");
        System.out.println("==================== CSDVIRT CSDVirtBatch ====================" + filename);
    }

    @Override
    public InputPartition[] planInputPartitions() {

        System.out.println("==================== CSDVIRT planInputPartitions ====================");

        return new CSDVirtInputPartition[]{new CSDVirtInputPartition()};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {

        System.out.println("==================== CSDVIRT createReaderFactory ====================");

        return new CSDVirtPartitionReaderFactory(readSchema_, filename);
    }
}

class CSDVirtInputPartition implements InputPartition {
    static final long serialVersionUID = 42424242L;

    public CSDVirtInputPartition() {
        
    }
}

class CSDVirtPartitionReaderFactory implements PartitionReaderFactory {
    static final long serialVersionUID = 4242L;
    private final StructType schema_;
    private final String filePath_;

    public CSDVirtPartitionReaderFactory(StructType schema, String fileName) {
        System.out.println("==================== CSDVIRT CSDVirtPartitionReaderFactory ====================" + fileName);
        schema_ = schema;
        filePath_ = fileName;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        System.out.println("==================== CSDVIRT PartitionReader ====================");

        return new CSDVirtPartitionReader(((CSDVirtInputPartition) partition), schema_, filePath_);
    }
}

class CSDVirtPartitionReader implements PartitionReader<InternalRow> {
    private final CSDVirtInputPartition partition;
    private final String fileName;
    private Iterator<String[]> iterator;
    // private CSVReader csvReader;
    // private List<Function> valueConverters;
    private StructType schema;
    private LinkedList<InternalRow> rows;
    private int num_rows;
    private int loop;

    private boolean temp;
    private final long csdvirtReaderPtr_;
    private List<Function> valueConverters;
    private StringBuffer stringBuffer;

    public CSDVirtPartitionReader(CSDVirtInputPartition partition, StructType schema, String fileName) {
        System.out.println("==================== CSDVIRT CSDVirtPartitionReader ====================" + fileName);
        this.partition = partition;
        this.fileName = fileName;
        this.schema = schema;
        StructField[] fields = schema.fields();
        System.out.println("==================== CSDVIRT CSDVirtPartitionReader ====================" + fields.length);

        CSDVirtLib lib = CSDVirtLib.get();
        
        if (lib == null) {
            System.out.println("[CSDVirt] library load fail!");
        } else {
            System.out.println("[CSDVirt] library load success!");
        }
        csdvirtReaderPtr_ = Native.csdvirt_create_reader(fileName + ".tbl");
        System.out.println("[CSDVirt] Create_reader success!");

        temp = true;
        this.valueConverters = ValueConverters.getConverters(schema);
        System.out.println("[CSDVirt] ValueConverters success!" + valueConverters.size());
        num_rows = fields.length;

        rows = new LinkedList<InternalRow>();
        stringBuffer = new StringBuffer();
        loop = 0;
    }

    @Override
    public boolean next() throws IOException {
        if (!rows.isEmpty()) {
            rows.pop();
            if (!rows.isEmpty()) return true;
        }

        loop++;
        /* if (loop > 3) {
            return false;
        } */

        // System.out.println("==================== CSDVIRT next ====================");
        int size = 1 * 1024 * 1024;
        ByteBuffer output_buffer = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
        int remain_size = Native.csdvirt_next_batch(csdvirtReaderPtr_, output_buffer, size);
        // System.out.println("remain_size:" + remain_size);

        // System.out.println("##: " + stringBuffer.toString());
        while (size > 0) {
            stringBuffer.append((char) output_buffer.get());
            size--;
        }
        String result = stringBuffer.toString();
        String[] lines = result.split("\\r?\\n");
    
        stringBuffer.delete(0, stringBuffer.length());    
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            String[] items = line.split("\\|");
            if (i == lines.length - 1) {
                // System.out.println("== Last Row ==" + i);
                // System.out.println(line);
                stringBuffer.append(line);
                // System.out.println("@@: " + stringBuffer.toString());
                break;
            }

            if (num_rows != items.length) {
                System.out.println("== row number do not match! ==" + i + " line length: " + lines.length);
                System.out.println(line);
                continue;
            }

            Object[] temp_obj = new Object[items.length];
            for (int j = 0; j < items.length; j++) {
                temp_obj[j] = valueConverters.get(j).apply(items[j]);
            }
            rows.add(InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(temp_obj).iterator()).asScala().toSeq()));
        }

        if (rows.isEmpty()) {// && remain_size == 0) {
            return false;
        }
        return true;
    }

    @Override
    public InternalRow get() {
        return rows.getFirst();
    }

    @Override
    public void close() throws IOException {
        System.out.println("==================== CSDVIRT close ====================");
        int test = Native.csdvirt_release_reader(csdvirtReaderPtr_);
        System.out.println("[CSDVirt] csdvirt_release_reader success!!");

        System.out.println("[CSDVirt] library use success!" + test);
        // Logger.getLogger("csdStorage.CSDSource").info("Reader 0x" + Long.toHexString(riscvReaderPtr_) + " finished:" + " reader_nano " + reader_nanos_ + " batch_nano " + batch_nanos_ + " buffer_nano " + (total_nanos_ - batch_nanos_));
    }
}