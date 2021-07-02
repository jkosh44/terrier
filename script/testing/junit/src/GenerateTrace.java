import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import moglib.Constants;
import moglib.MogDb;
import moglib.MogSqlite;


/**
 * class that convert sql statements to trace format first, establish a local postgresql database
 * second, start the database server with "pg_ctl -D /usr/local/var/postgres start" third, modify
 * the url, user and password string to match the database you set up finally, provide path to a
 * file, run generateTrace with the file path as argument input file format: sql statements, one per
 * line output file: to be tested by TracefileTest
 */
public class GenerateTrace {

    public static void main(String[] args) throws Throwable {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        String inputPath = args[0];
        String jdbcUrl = args[1];
        String dbUser = args[2];
        String dbPassword = args[3];
        String outputFile = args[4];

        File inputFile = new File(inputPath);
        System.out.println("File path: " + inputPath);
        MogSqlite mog = new MogSqlite(inputFile);

        MogDb db = new MogDb(jdbcUrl, dbUser, dbPassword);

        try (
            Connection conn = db.getDbTest().newConn();
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            FileWriter writer = new FileWriter(new File(Constants.DEST_DIR, outputFile));
        ) {
            TestUtility.removeAllExistingTables(mog, conn);
            generateTrace(br, writer, conn, mog, db);
        }
    }

    private static void generateTrace(BufferedReader br, FileWriter writer, Connection conn,
        MogSqlite mog, MogDb db) throws Throwable {

        String line;
        int expectedResultNum = -1;
        boolean includeResult = false;

        while (null != (line = br.readLine())) {
            line = line.trim();

            if (line.startsWith(Constants.HASHTAG)) {
                TestUtility.writeToFile(writer, line);
                if (line.contains(Constants.NUM_OUTPUT_FLAG)) {
                    // case for specifying the expected number of outputs
                    String[] arr = line.split(" ");
                    expectedResultNum = Integer.parseInt(arr[arr.length - 1]);
                } else if (line.contains(Constants.EXPECTED_OUTPUT_FLAG)) {
                    // case for including exact result in mog.queryResult
                    includeResult = true;
                }
                continue;
            }

            // execute sql statement
            Statement statement;
            try {
                statement = conn.createStatement();
                statement.execute(line);
            } catch (Exception e) {
                System.err.println(
                    "Error executing SQL Statement: '" + line + "'; " + e.getMessage());
                TestUtility.writeToFile(writer, Constants.STATEMENT_ERROR, line);
                continue;
            }

            if (line.startsWith("SELECT") || line.toLowerCase().startsWith("with")) {
                processSelectAndCTEStatements(line, statement, expectedResultNum,
                    includeResult,
                    writer, mog, db);
                includeResult = false;
            } else {
                String label = Constants.STATEMENT_OK;
                // other sql statements
                int rs = statement.getUpdateCount();
                // check if expected number is equal to update count
                if (expectedResultNum >= 0 && expectedResultNum != rs) {
                    label = Constants.STATEMENT_ERROR;
                }
                TestUtility.writeToFile(writer, label, line, "\n");
            }
            expectedResultNum = -1;
        }
    }

    private static void processSelectAndCTEStatements(String line, Statement statement,
        int expectedResultNum, boolean includeResult, FileWriter writer, MogSqlite mog, MogDb db)
        throws SQLException, IOException {
        ResultSet rs = statement.getResultSet();

        if (line.toLowerCase().startsWith("with") && null == rs) {
            // We might have a query that begins with `WITH` that has a null result set
            int updateCount = statement.getUpdateCount();
            String label = Constants.STATEMENT_OK;
            // check if expected number is equal to update count
            if (expectedResultNum >= 0 && expectedResultNum != updateCount) {
                label = Constants.STATEMENT_ERROR;
            }
            TestUtility.writeToFile(writer, label, line);
            writer.write('\n');
            return;
        }

        String typeString = extractColumnTypesFromResult(rs, db);
        String sortOption = determineSortOrder(line, mog);
        String querySort = Constants.QUERY + " " + typeString + " " + sortOption;

        List<String> res = mog.processResults(rs);
        String queryResult = processQueryResults(res, expectedResultNum, includeResult);

        TestUtility.writeToFile(writer, querySort, line, Constants.SEPARATION, queryResult);
        if (!res.isEmpty()) {
            writer.write('\n');
        }
    }

    private static String extractColumnTypesFromResult(ResultSet rs, MogDb db) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        StringBuilder typeString = new StringBuilder();
        for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
            String colTypeName = rsmd.getColumnTypeName(i);
            MogDb.DbColumnType colType = db.getDbTest().getDbColumnType(colTypeName);
            if (colType == MogDb.DbColumnType.FLOAT) {
                typeString.append("R");
            } else if (colType == MogDb.DbColumnType.INTEGER) {
                typeString.append("I");
            } else if (colType == MogDb.DbColumnType.TEXT) {
                typeString.append("T");
            } else {
                System.out.println(colTypeName + " column invalid");
            }
        }
        return typeString.toString();
    }

    private static String determineSortOrder(String line, MogSqlite mog) {
        if (line.contains("ORDER BY")) {
            // These rows are already sorted by the SQL and need to match exactly
            mog.sortMode = "nosort";
        } else {
            // Need to create a canonical ordering...
            mog.sortMode = "rowsort";
        }
        return mog.sortMode;
    }

    private static String processQueryResults(List<String> res, int expected_result_num,
        boolean include_result) {
        String queryResult = "";
        // when include_result is true, set queryResult to be exact result instead of hash
        // set queryResult to be exact result instead of hash when
        // result size is smaller than Constants.DISPLAY_RESULT_SIZE
        if (include_result) {
            queryResult = String.join("\n", res).trim();
        } else {
            // compute the hash
            String hash = TestUtility.getHashFromDb(res);
            // if expected number of results is specified
            if (expected_result_num >= 0) {
                queryResult = "Expected " + expected_result_num + " values hashing to " + hash;
            } else if (res.size() < Constants.DISPLAY_RESULT_SIZE) {
                // set queryResult to be exact result instead of hash when
                // result size is smaller than Constants.DISPLAY_RESULT_SIZE
                queryResult = String.join("\n", res).trim();
            } else {
                // set queryResult to format x values hashing to xxx
                queryResult = res.size() + " values hashing to " + hash;
            }
        }
        return queryResult;
    }
}
