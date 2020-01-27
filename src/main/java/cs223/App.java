package cs223;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import cs223.Common.Constants;
import cs223.Engine.Controller;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;


public class App {

    public static boolean needCleanup = true;

    public static void main( String args[] ) throws ClassNotFoundException, FileNotFoundException, SQLException, InterruptedException, ExecutionException {
        Class.forName("org.postgresql.Driver");
        Class.forName("com.mysql.jdbc.Driver");

        if(needCleanup){
            Connection conn = DriverManager.getConnection("jdbc:"+ Constants.dbType.getDatabase() +":"+Constants.url+Constants.params,Constants.username,Constants.password);
            ScriptRunner sr = new ScriptRunner(conn);
            sr.setLogWriter(null);
            String dropPath = Constants.dataRootPath+"\\schema\\drop.sql";
            sr.runScript(new BufferedReader(new FileReader(dropPath)));
            sr.closeConnection();
            conn.close();
        }

        final ActorSystem<Controller.Command> system =
                ActorSystem.create(Controller.create(), "engine");
        system.tell(new Controller.Setup(Constants.maxThreads, Constants.dbType,Constants.url,Constants.params,Constants.username,Constants.password,Constants.isolationLevel));
        CompletionStage<Controller.Reply> result = AskPattern.ask(system, replyTo -> new Controller.RunBenchmark(Constants.benchmarkType,Constants.minInsertions,Constants.maxInsertions,Constants.transactionInterval,replyTo), Duration.ofHours(1),system.scheduler());
        result.toCompletableFuture().get();
        system.terminate();
    }
}
