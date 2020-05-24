package io.github.streamingwithflink.batchtableaggregation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.table.api.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.*;
//import org.apache.flink.table.api.java.*;
import org.apache.flink.table.api.java.*;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.*;
import org.apache.flink.types.Row;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

// https://riptutorial.com/apache-flink/example/27901/simple-aggregation-from-a-csv
// https://stackoverflow.com/questions/49096358/apache-flink-unable-to-convert-the-table-object-to-dataset-object
public class TableExample {
    public static void main( String[] args ) throws Exception{
        // create the environments
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment( env );

        // get the path to the file in resources folder
        String peoplesPath = TableExample.class.getClassLoader().getResource( "people.csv" ).getPath();
        // load the csv into a table
        CsvTableSource tableSource = new CsvTableSource(
                peoplesPath,
                "id,last_name,country,gender".split( "," ),
                new TypeInformation[]{ Types.INT(), Types.STRING(), Types.STRING(), Types.STRING() } );
        // register the table and scan it
        tableEnv.registerTableSource( "peoples", tableSource );
        Table peoples = tableEnv.scan( "peoples" );

        // aggregation using chain of methods
        Table countriesCount = peoples.groupBy( "country" ).select( "country, id.count" );
        DataSet<Row> result1 = tableEnv.toDataSet( countriesCount, Row.class );
        result1.print();

        // aggregation using SQL syntax
        Table countriesAndGenderCount = tableEnv.sqlQuery(
                "select country, gender, count(id) from peoples group by country, gender" );

        DataSet<Row> result2 = tableEnv.toDataSet( countriesAndGenderCount, Row.class );
        result2.print();
    }
}
