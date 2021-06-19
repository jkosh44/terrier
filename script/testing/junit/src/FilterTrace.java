import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import moglib.Constants;
import moglib.MogDb;
import moglib.MogSqlite;

/**
 * class that filter out desired trace
 * check out instruction at junit/README.md
 */
public class FilterTrace {
    public static void main(String[] args) throws Throwable {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        String inputFilePath = args[0];
        String jdbcUrl = args[1];
        String user = args[2];
        String password = args[3];
        String[] skip_list = args[4].split(",");

        File file = new File(inputFilePath);
        System.out.println("File path: " + inputFilePath);

        for(String i: skip_list){
            System.out.println(i);
        }

        MogSqlite mog = new MogSqlite(file);
        MogDb db = new MogDb(jdbcUrl, user, password);

        // create output file
        File outputFile = new File(Constants.DEST_DIR, args[5]);

        try(
            Connection conn = db.getDbTest().newConn();
            FileWriter writer = new FileWriter(outputFile)
        ) {
            TestUtility.removeExistingTable(mog, conn);
            filterTrace(mog, skip_list, conn, writer);
        }
    }

    private static void filterTrace(MogSqlite mog, String[] skip_list, Connection conn,
        FileWriter writer) throws Exception {
        boolean skip_flag = false;
        while (mog.next()) {
            String cur_sql = mog.sql.trim();
            for(String comment : mog.comments) {
                if (comment.contains(Constants.SKIPIF) || comment.contains(Constants.ONLYIF)) {
                    skip_flag = true;
                    break;
                }
            }
            // filter out nested SELECT statements
            if(getFrequency(cur_sql, "SELECT")>1){
                mog.comments.clear();
                continue;
            }
            // the code below remove the queries that contain any skip keyword
            for(String skip_word : skip_list) {
                if (cur_sql.contains(skip_word)) {
                    skip_flag = true;
                    break;
                }
            }
            if(skip_flag){
                skip_flag = false;
                mog.comments.clear();
                continue;
            }
            TestUtility.writeToFile(writer, mog.queryFirstLine);
            TestUtility.writeToFile(writer, cur_sql);
            if(mog.queryFirstLine.contains(Constants.QUERY)){
                TestUtility.writeToFile(writer, Constants.SEPARATION);
                try{
                    Statement statement = conn.createStatement();
                    statement.execute(cur_sql);
                    ResultSet rs = statement.getResultSet();
                    List<String> res = mog.processResults(rs);
                    if(res.size()>0){
                        if(res.size()<Constants.DISPLAY_RESULT_SIZE) {
                            for(String i:mog.queryResults){
                                TestUtility.writeToFile(writer, i);
                            }
                        }else {
                            String hash = TestUtility.getHashFromDb(res);
                            String queryResult = res.size() + " values hashing to " + hash;
                            TestUtility.writeToFile(writer, queryResult);
                        }
                    }
                }catch(Throwable e){
                    throw new Exception(e.getMessage() + ": " + cur_sql);
                }
            }else{
                try{
                    Statement statement = conn.createStatement();
                    statement.execute(cur_sql);
                }catch(Throwable e){
                    throw new Exception(e.getMessage() + ": " + cur_sql);
                }
            }
            writer.write('\n');
            mog.comments.clear();
            mog.queryResults.clear();
        }
    }

    private static int getFrequency(String sql, String keyword){
        int num = 0;
        String[] arr = sql.split(" ");
        for(String i:arr){
            if(i.contains(keyword)){
                num += 1;
            }
        }
        return num;
    }
}
