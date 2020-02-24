package cs223.Engine;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import com.mysql.cj.jdbc.MysqlXid;
import cs223.Common.Constants;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.postgresql.core.BaseConnection;
import org.postgresql.xa.PGXAConnection;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class Agent extends AbstractBehavior<Agent.Command> {

    PGXAConnection XAconn;
    ActorRef<Coordinator.Command> coordinator;
    int bid;
    int tid = 0;
    boolean isNowCommitPhase = false;
    Xid currentId;

    public static Behavior<Command> create(ActorRef<Coordinator.Command> coordinator, int port, int id) {
        return Behaviors.setup(s -> Behaviors.setup(c -> new Agent(c, coordinator,port,id)));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Prepare.class, this::onPrepare)
                .onMessage(Commit.class,this::onCommit)
                .onMessage(Abort.class,this::onAbort)
                .build();
    }

    interface Command{}

    public static final class Prepare implements Command {
        final String[] payload;
        final int gid;
        public Prepare(String[] payload, int gid) {
            this.payload = payload;
            this.gid = gid;
        }
    }

    public static final class Commit implements Command{
        ActorRef<Coordinator.Ack> replyTo;
        public Commit(ActorRef<Coordinator.Ack> replyTo){
            this.replyTo = replyTo;
        }

    }

    public static final class Abort implements Command{
        ActorRef<Coordinator.Ack> replyTo;
        public Abort(ActorRef<Coordinator.Ack> replyTo){
            this.replyTo = replyTo;
        }

    }

    public static final class AgentReply implements Coordinator.Command{
        final int command;
        final Xid id;
        public AgentReply(int command,Xid id){
            this.command = command;
            this.id = id;
        }
    }

    private Behavior<Command> onCommit(Commit commit){
        commit.replyTo.tell(Coordinator.Ack.INSTANCE);
        if(!isNowCommitPhase)
            return Behaviors.same();
        try {
            XAconn.commit(currentId,false);
        } catch (XAException e) {
            e.printStackTrace();
        }
        isNowCommitPhase = false;
        return Behaviors.same();
    }


    private Behavior<Command> onAbort(Abort abort){
        abort.replyTo.tell(Coordinator.Ack.INSTANCE);
        if(!isNowCommitPhase)
            return Behaviors.same();
        try {
            XAconn.rollback(currentId);
        } catch (XAException e) {
            e.printStackTrace();
        }
        isNowCommitPhase = false;
        return Behaviors.same();
    }



    private Behavior<Command> onPrepare(Prepare task) {
        currentId = new MysqlXid(intToByteArray(task.gid),intToByteArray(bid),tid);
        tid++;
        try {
            //start of the first phase
            XAconn.start(currentId, XAResource.TMNOFLAGS);
            Statement stmt = XAconn.getConnection().createStatement();
            for (String i : task.payload) {
                stmt.addBatch(i);
            }
            stmt.executeBatch();
            XAconn.end(currentId, XAResource.TMSUCCESS);
            int result = XAconn.prepare(currentId);
            //vote YES
            coordinator.tell(new AgentReply(result,currentId));
        }catch(Exception e){
            e.printStackTrace();
            //vote NO
            coordinator.tell(new AgentReply(Constants.ABORT,currentId));
        }
        //start of the second phase
        isNowCommitPhase = true;
        return Behaviors.same();
    }

    private Agent(ActorContext<Command> context, ActorRef<Coordinator.Command> coordinator, int port, int id) {
        super(context);
        this.bid = id;
        this.coordinator = coordinator;
        try {
            //drop tables
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:"+port+"/"+Constants.url,Constants.username,Constants.password);
            ScriptRunner sr = new ScriptRunner(conn);
            sr.setLogWriter(null);
            String dropPath = Constants.dataRootPath+"\\schema\\"+Constants.dbType.getDatabase()+"_drop.sql";
            sr.runScript(new BufferedReader(new FileReader(dropPath)));
            String schemaPath = Constants.dataRootPath + "\\schema\\create.sql";
            sr.runScript(new BufferedReader(new FileReader(schemaPath)));
            String metadataPath = Constants.dataRootPath + "\\data\\" + Constants.benchmarkType + "\\metadata.sql";
            sr.runScript(new BufferedReader(new FileReader(metadataPath)));
            XAconn = new PGXAConnection((BaseConnection)conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static byte[] intToByteArray(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value};
    }
}