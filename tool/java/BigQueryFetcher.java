import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.FieldValueList;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Fetch contract data from Google BigQuery and write JSON lines.
 *
 * Usage:
 *  java -cp <classpath> BigQueryFetcher --dataset <table> --start-block <start> --end-block <end> --output <file>
 */
public class BigQueryFetcher {
    public static void main(String[] args) throws Exception {
        String dataset = "bigquery-public-data.crypto_ethereum.contracts";
        long startBlock = 0;
        long endBlock = 0;
        String output = "contracts.jsonl";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dataset":
                    dataset = args[++i];
                    break;
                case "--start-block":
                    startBlock = Long.parseLong(args[++i]);
                    break;
                case "--end-block":
                    endBlock = Long.parseLong(args[++i]);
                    break;
                case "--output":
                    output = args[++i];
                    break;
                default:
                    System.err.println("Unknown arg: " + args[i]);
            }
        }

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        String query = String.format(
            "SELECT address, bytecode, block_number FROM `%s` " +
            "WHERE block_number >= %d AND block_number <= %d ORDER BY block_number",
            dataset, startBlock, endBlock);
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(query).build();
        TableResult result = bigquery.query(config);
        try (PrintWriter out = new PrintWriter(new FileWriter(output))) {
            for (FieldValueList row : result.iterateAll()) {
                String address = row.get("address").getStringValue();
                String bytecode = row.get("bytecode").getStringValue();
                long blockNumber = row.get("block_number").getLongValue();
                String line = String.format(
                    "{\"Address\":\"%s\",\"ByteCode\":\"%s\",\"BlockNumber\":%d}",
                    address, bytecode, blockNumber);
                out.println(line);
            }
        }
    }
}
