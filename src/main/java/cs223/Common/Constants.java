package cs223.Common;

public class Constants {

    public static final String dataRootPath = "D:\\project1";

    public static DatabaseType dbType = DatabaseType.Postgres;

    public static String url = "cs223";

    public static final String username = "cs223";

    public static final String password = "cs223";

    public static final int minInsertions = 1;

    public static final int maxInsertions = 10;

    public static String benchmarkType = "low_concurrency";

    public static final long transactionInterval = 1000;  //1000ms

    public static int numTransactions = 10000;

    public static int ABORT = -1;

    public static int startPort = 5432;



}
