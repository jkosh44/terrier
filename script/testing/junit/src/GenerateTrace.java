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
    String inputFilePath = args[0];
    String jdbcUrl = args[1];
    String user = args[2];
    String password = args[3];
    String outputFilePath = args[4];

    File file = new File(inputFilePath);
    System.out.println("Trace file path: " + inputFilePath);
    MogSqlite mog = new MogSqlite(file);
    MogDb db = new MogDb(jdbcUrl, user, password);

    try (Connection conn = db.getDbTest().newConn()) {
      TestUtility.removeExistingTable(mog, conn);
      generateTrace(file, outputFilePath, conn, mog, db);
    }
  }

  private static void generateTrace(File inputFile, String outputFilePath, Connection conn,
      MogSqlite mog, MogDb db) throws Throwable {
    String line;
    String label;
    Statement statement = null;
    try (
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        FileWriter writer = new FileWriter(new File(Constants.DEST_DIR, outputFilePath))
    ) {
      int expected_result_num = -1;
      boolean include_result = false;
      while (null != (line = br.readLine())) {
        line = line.trim();

        if (line.startsWith(Constants.HASHTAG)) {
          TestUtility.writeToFile(writer, line);
          if (line.contains(Constants.NUM_OUTPUT_FLAG)) {
            // case for specifying the expected number of outputs
            String[] arr = line.split(" ");
            expected_result_num = Integer.parseInt(arr[arr.length - 1]);
          } else if (line.contains(Constants.FAIL_FLAG)) {
            // case for expecting the query to fail
            // I'm almost positive that this case does nothing
            label = Constants.STATEMENT_ERROR;
          } else if (line.contains(Constants.EXPECTED_OUTPUT_FLAG)) {
            // case for including exact result in mog.queryResult
            include_result = true;
          }
          continue;
        }

        // execute sql statement
        try {
          statement = conn.createStatement();
          statement.execute(line);
          label = Constants.STATEMENT_OK;
        } catch (Exception e) {
          System.err.println("Error executing SQL Statement: '" + line + "'; " + e.getMessage());
          TestUtility.writeToFile(writer, Constants.STATEMENT_ERROR);
          TestUtility.writeToFile(writer, line);
          continue;
        }

        if (line.startsWith("SELECT") || line.toLowerCase().startsWith("with")) {
          processSelectAndCTEStatements(line, statement, expected_result_num, include_result,
              writer, mog, db);
          include_result = false;
        } else {
          // other sql statements
          int rs = statement.getUpdateCount();
          // check if expected number is equal to update count
          if (expected_result_num >= 0 && expected_result_num != rs) {
            label = Constants.STATEMENT_ERROR;
          }
          TestUtility.writeToFile(writer, label);
          TestUtility.writeToFile(writer, line);
          writer.write('\n');
        }
        expected_result_num = -1;
      }
    }
  }

  private static void processSelectAndCTEStatements(String line, Statement statement,
      int expected_result_num, boolean include_result, FileWriter writer, MogSqlite mog, MogDb db)
      throws SQLException, IOException {
    ResultSet rs = statement.getResultSet();
    if (line.toLowerCase().startsWith("with") && null == rs) {
      // We might have a query that begins with `WITH` that has a null result set
      int updateCount = statement.getUpdateCount();
      String label = Constants.STATEMENT_OK;
      // check if expected number is equal to update count
      if (expected_result_num >= 0 && expected_result_num != updateCount) {
        label = Constants.STATEMENT_ERROR;
      }
      TestUtility.writeToFile(writer, label);
      TestUtility.writeToFile(writer, line);
      writer.write('\n');
      return;
    }

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

    String sortOption;
    if (line.contains("ORDER BY")) {
      // These rows are already sorted by the SQL and need to match exactly
      sortOption = "nosort";
      mog.sortMode = "nosort";
    } else {
      // Need to create a canonical ordering...
      sortOption = "rowsort";
      mog.sortMode = "rowsort";
    }
    String query_sort = Constants.QUERY + " " + typeString + " " + sortOption;
    TestUtility.writeToFile(writer, query_sort);
    TestUtility.writeToFile(writer, line);
    TestUtility.writeToFile(writer, Constants.SEPARATION);
    List<String> res = mog.processResults(rs);
    // compute the hash
    String hash = TestUtility.getHashFromDb(res);
    String queryResult = "";
    // when include_result is true, set queryResult to be exact result instead of hash
    if (include_result) {
      for (String i : res) {
        queryResult += i;
        queryResult += "\n";
      }
      queryResult = queryResult.trim();
    } else {
      // if expected number of results is specified
      if (expected_result_num >= 0) {
        queryResult = "Expected " + expected_result_num + " values hashing to " + hash;
      } else {
        if (res.size() > 0) {
          // set queryResult to format x values hashing to xxx
          queryResult = res.size() + " values hashing to " + hash;
        }
        // set queryResult to be exact result instead of hash when
        // result size is smaller than Constants.DISPLAY_RESULT_SIZE
        if (res.size() < Constants.DISPLAY_RESULT_SIZE) {
          queryResult = "";
          for (String i : res) {
            queryResult += i;
            queryResult += "\n";
          }
          queryResult = queryResult.trim();
        }
      }
    }
    TestUtility.writeToFile(writer, queryResult);
    if (res.size() > 0) {
      writer.write('\n');
    }
  }
}
