package com.rvce.hbase_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.scheduling.annotation.Async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class HBaseBulkOperations {

    private final TableName employeesTable = TableName.valueOf("employees");
    private final String dataCF = "data";
    private AtomicInteger index = new AtomicInteger(3000000);


    @Async
    void bulkPut(Table table) throws IOException {
        Stream.iterate(0, i -> i).limit(1000).parallel().forEach(integer -> {
            List<Put> employeesList = new ArrayList<>();
            Stream.iterate(0, j -> j).limit(1000).forEach(n -> {
                int i = index.getAndIncrement();
                System.out.println("Inserting " + i);
                Put p = new Put(Bytes.toBytes("Student" + i));
                p.addColumn(dataCF.getBytes(), "name".getBytes(), Bytes.toBytes("emp" + i));
                p.addColumn(dataCF.getBytes(), "id".getBytes(), Bytes.toBytes("id" + i));
                employeesList.add(p);
            });
            try {
                table.put(employeesList);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println("Done. ");
    }

    void run(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
            Arrays.stream(admin.listTableNamesByNamespace("default")).forEach(System.out::println);
            Table table = connection.getTable(employeesTable);
            bulkPut(table);

        }
    }


}