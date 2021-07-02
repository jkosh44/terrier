import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import moglib.Constants;
import moglib.MogDb;
import moglib.MogSqlite;

/**
 * class that filter out desired trace check out instruction at junit/README.md
 */
public class FilterTrace {

    public static void main(String[] args) throws Exception {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        String inputPath = args[0];
        String jdbcUrl = args[1];
        String dbUser = args[2];
        String dbPassword = args[3];
        List<String> skipList = Arrays.asList(args[4].split(","));
        String outputFileName = args[5];

        File inputFile = new File(inputPath);
        System.out.println("File path: " + inputPath);
        MogSqlite mog = new MogSqlite(inputFile);
        File outputFile = new File(Constants.DEST_DIR, outputFileName);

        for (String i : skipList) {
            System.out.println(i);
        }

        MogDb db = new MogDb(jdbcUrl, dbUser, dbPassword);

        try (
            FileWriter writer = new FileWriter(outputFile);
            Connection conn = db.getDbTest().newConn();
        ) {
            TestUtility.removeAllExistingTables(mog, conn);
            filterTrace(mog, conn, writer, skipList);
        }
    }

    private static void filterTrace(MogSqlite mog, Connection conn, FileWriter writer,
        List<String> skipList) throws Exception {
        boolean skipFlag = false;
        while (mog.next()) {
            String curSql = mog.sql.trim();
            if (mog.comments.stream().anyMatch(FilterTrace::commentContainsSkipFlag)) {
                skipFlag = true;
            }

            // filter out nested SELECT statements
            if (getFrequency(curSql, "SELECT") > 1) {
                mog.comments.clear();
                continue;
            }

            // the code below remove the queries that contain any skip keyword
            if (skipList.stream().anyMatch(curSql::contains)) {
                skipFlag = true;
            }

            if (skipFlag) {
                skipFlag = false;
                mog.comments.clear();
                continue;
            }

            TestUtility.writeToFile(writer, mog.queryFirstLine, curSql);

            executeSql(mog, writer, conn, curSql);

            writer.write('\n');
            mog.comments.clear();
            mog.queryResults.clear();
        }
    }

    private static boolean commentContainsSkipFlag(String comment) {
        return comment.contains(Constants.SKIPIF) || comment.contains(Constants.ONLYIF);
    }

    private static int getFrequency(String sql, String keyword) {
        int num = 0;
        String[] arr = sql.split(" ");
        for (String i : arr) {
            if (i.contains(keyword)) {
                num += 1;
            }
        }
        return num;
    }

    private static void executeSql(MogSqlite mog, FileWriter writer, Connection conn, String curSql)
        throws Exception {

        try {
            Statement statement = conn.createStatement();
            statement.execute(curSql);

            if (mog.queryFirstLine.contains(Constants.QUERY)) {
                TestUtility.writeToFile(writer, Constants.SEPARATION);
                ResultSet rs = statement.getResultSet();
                List<String> res = mog.processResults(rs);
                if (!res.isEmpty()) {
                    if (res.size() < Constants.DISPLAY_RESULT_SIZE) {
                        for (String i : mog.queryResults) {
                            TestUtility.writeToFile(writer, i);
                        }
                    } else {
                        String hash = TestUtility.getHashFromDb(res);
                        String queryResult = res.size() + " values hashing to " + hash;
                        TestUtility.writeToFile(writer, queryResult);
                    }
                }
            }
        } catch (SQLException e) {
            throw new Exception(e.getMessage() + ": " + curSql);
        }
    }
}
