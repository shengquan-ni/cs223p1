package cs223.Common;

//TRANSACTION_NONE = 0
//TRANSACTION_READ_UNCOMMITTED = 1
//TRANSACTION_READ_COMMITTED = 2
//TRANSACTION_REPEATABLE_READ = 4
//TRANSACTION_SERIALIZABLE = 8


import javax.transaction.xa.XAResource;

public class Constants {

    public static final String dataRootPath = "D:\\project1";

    public static DatabaseType dbType = DatabaseType.MySQL;

    public static String url = "//localhost:3306/cs223";

    public static final String params = "";

    public static final String username = "cs223";

    public static final String password = "cs223";

    public static int isolationLevel = 1;

    public static final int maxThreads = 8;

    public static final int minInsertions = 1;

    public static final int maxInsertions = 10;

    public static String benchmarkType = "low_concurrency";

    public static final long transactionInterval = 1000;  //1000ms

    public static int numTransactions = 10000;

    public static int COMMIT = XAResource.XA_OK;

    public static int ABORT = -1;


}
