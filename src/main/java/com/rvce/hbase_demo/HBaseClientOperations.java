package com.rvce.hbase_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

class HBaseClientOperations {

    private final TableName studentTable = TableName.valueOf("student_table");
    private final String personalDetailsCF = "personal_details";
    private final String academicsDetailsCF = "academics_details";

    private final byte[] student1Row = Bytes.toBytes("Student1");
    private final byte[] student2Row = Bytes.toBytes("Student2");
    private final byte[] student3Row = Bytes.toBytes("Student3");
    private final byte[] nameQualifier = Bytes.toBytes("Name");
    private final byte[] semQualifier = Bytes.toBytes("Sem");
    private final byte[] percentageQualifier = Bytes.toBytes("Marks");


    private void delete(Table table) throws IOException {

        final byte[] rowToBeDeleted =  Bytes.toBytes("RowToBeDeleted");
        System.out.println("\n*** DELETE ~Insert data and then delete it~ ***");

        System.out.println("Inserting a data to be deleted later.");
        Put put = new Put(rowToBeDeleted);
        byte[] cellData = Bytes.toBytes("cell_data");
        put.addColumn(personalDetailsCF.getBytes(), nameQualifier, cellData);
        table.put(put);

        Get get = new Get(rowToBeDeleted);
        Result result = table.get(get);
        byte[] value = result.getValue(personalDetailsCF.getBytes(), nameQualifier);
        System.out.println("Fetch the data: " + Bytes.toString(value));
        assert Arrays.equals(cellData, value);

        System.out.println("Deleting");
        Delete delete = new Delete(rowToBeDeleted);
        delete.addColumn(personalDetailsCF.getBytes(), nameQualifier);
        table.delete(delete);

        result = table.get(get);
        value = result.getValue(personalDetailsCF.getBytes(), nameQualifier);
        System.out.println("Fetch the data: " + Bytes.toString(value));
        assert Arrays.equals(null, value);

        System.out.println("Done. ");
    }


    private void filters(Table table) throws IOException {
        System.out.println("\n*** FILTERS ~ scanning with filters to fetch a row of which key is larger than \"Row1\"~ ***");
        Filter filter1 = new PrefixFilter(student1Row);
        Filter filter2 = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(
            percentageQualifier));

        List<Filter> filters = Arrays.asList(filter1, filter2);

        Scan scan = new Scan();
        scan.setFilter(new FilterList(Operator.MUST_PASS_ALL, filters));

        try (ResultScanner scanner = table.getScanner(scan)) {
            int i = 0;
            for (Result result : scanner) {
                System.out.println("Filter " + scan.getFilter() + " matched row: " + result);
                i++;
            }
            assert i == 2 : "This filtering sample should return 1 row but was " + i + ".";
        }
        System.out.println("Done. ");
    }

    private void get(Table table) throws IOException {
        Get g = new Get(student1Row);
        Result r = table.get(g);
        byte[] value = r.getValue(personalDetailsCF.getBytes(), nameQualifier);

        System.out.println("Fetched value: " + Bytes.toString(value));
        System.out.println("Done. ");
    }

    private void put(Admin admin, Table table) throws IOException {

        // Row1
        Put p = new Put(student1Row);
        p.addColumn(personalDetailsCF.getBytes(), nameQualifier, Bytes.toBytes("ram"));
        p.addColumn(academicsDetailsCF.getBytes(), semQualifier, Bytes.toBytes(1));
        p.addColumn(academicsDetailsCF.getBytes(), percentageQualifier, Bytes.toBytes(95));
        table.put(p);

        // Row2
        p = new Put(student2Row);
        p.addColumn(personalDetailsCF.getBytes(), nameQualifier, Bytes.toBytes("kumar"));
        p.addColumn(academicsDetailsCF.getBytes(), semQualifier, Bytes.toBytes(1));
        p.addColumn(academicsDetailsCF.getBytes(), percentageQualifier, Bytes.toBytes(90));
        p.addColumn(academicsDetailsCF.getBytes(), semQualifier, Bytes.toBytes(2));
        p.addColumn(academicsDetailsCF.getBytes(), percentageQualifier, Bytes.toBytes(99));
        table.put(p);

        admin.disableTable(studentTable);
        try {
            HColumnDescriptor desc = new HColumnDescriptor(student1Row);
            admin.addColumnFamily(studentTable, desc);
            System.out.println("Success.");
        } catch (Exception e) {
            System.out.println("Failed.");
            System.out.println(e.getMessage());
        } finally {
            admin.enableTable(studentTable);
        }
        System.out.println("Done. ");
    }

    void run(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
            Arrays.stream(admin.listTableNamesByNamespace("default")).forEach(System.out::println);
            Table table = connection.getTable(studentTable);
            put(admin, table);
            get(table);
            scan(table);
            filters(table);
            delete(table);
        }
    }

    private void scan(Table table) throws IOException {
        System.out.println("\n*** SCAN example ~fetching data in Family1:Qualifier1 ~ ***");

        Scan scan = new Scan();

        scan.addColumn(personalDetailsCF.getBytes(), nameQualifier);

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner)
                System.out.println("Found row: " + result);
        }
        System.out.println("Done.");
    }
}