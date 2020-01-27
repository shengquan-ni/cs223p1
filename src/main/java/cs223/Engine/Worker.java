package cs223.Engine;


import akka.actor.AbstractActor;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.typed.ActorRef;
import cs223.Common.DatabaseType;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Worker extends AbstractActor {

    Connection conn;
    ActorRef<Observer.Command> observer;
    long aggregatedExecutionTime = 0;
    long numTransactions = 0;
    long numQueries = 0;
    long aggregatedQueryTime = 0;

    static Props props(DatabaseType dbType, String url, String params, String username, String password, int isolationLevel, ActorRef<Observer.Command> observer) {
        return Props.create(Worker.class, () -> new Worker(dbType,url,params,username,password,isolationLevel,observer));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        WorkerTask.class,
                        this::onReceiveTask)
                .match(
                        End.class,
                        this::onEnd)
                .build();
    }

    interface Command{}

    public static final class WorkerTask implements Command {
        final String[] payload;
        final boolean isQuery;

        public WorkerTask(String[] payload,boolean isQuery) {
            this.payload = payload;
            this.isQuery = isQuery;
        }
    }

    public static final class End implements Command {

        public End() {

        }
    }

    private void onReceiveTask(WorkerTask task) throws SQLException {
        long start;
        long interval;
        if(!task.isQuery){
            Statement stmt = conn.createStatement();
            for(String i:task.payload){
                stmt.addBatch(i);
            }
            start = System.nanoTime();
            stmt.executeBatch();
            conn.commit();
            interval = System.nanoTime() - start;
        }else{
            assert(task.payload.length == 1);
            Statement stmt = conn.createStatement();
            start = System.nanoTime();
            stmt.execute(task.payload[0]);
            conn.commit();
            interval = System.nanoTime() - start;
            aggregatedQueryTime += interval;
            numQueries++;
        }
        aggregatedExecutionTime += interval;
        numTransactions++;
    }

    private void onEnd(End msg){
        observer.tell(new Observer.Report(numTransactions,aggregatedExecutionTime,numQueries,aggregatedQueryTime));
        self().tell(PoisonPill.getInstance(),null);
    }



    private Worker(DatabaseType dbType, String url, String params, String username, String password, int isolationLevel, ActorRef<Observer.Command> observer) throws SQLException {
        this.observer = observer;
        conn = DriverManager.getConnection("jdbc:"+dbType.getDatabase()+":"+url+params,username,password);
        conn.setTransactionIsolation(isolationLevel);
        conn.setAutoCommit(false);

    }

}
