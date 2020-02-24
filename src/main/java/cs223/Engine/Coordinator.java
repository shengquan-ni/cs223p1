package cs223.Engine;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import cs223.Common.Constants;
import cs223.Transaction.TransactionManager;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Coordinator extends AbstractBehavior<Coordinator.Command> {

    private ActorRef<Reply> replyTo;
    ArrayList<ActorRef<Agent.Command>> agents;
    ArrayList<ArrayList<String>> partitions;
    TransactionManager tm;
    int transactionCounter = 0;
    int numVoteReceived = 0;
    boolean voteToAbort = false;

    public interface Command{}


    public static class RunBenchmark implements Command {
        String benchmarkType;
        int minInsertions;
        int maxInsertions;
        long transactionInterval;
        ActorRef<Reply> replyTo;
        int numAgents;

        public RunBenchmark(String benchmarkType, int minInsertions, int maxInsertions, long transactionInterval, ActorRef<Reply> replyTo, int numAgents) {
            this.benchmarkType = benchmarkType;
            this.minInsertions = minInsertions;
            this.maxInsertions = maxInsertions;
            this.transactionInterval = transactionInterval;
            this.replyTo = replyTo;
            this.numAgents = numAgents;

        }
    }

    public interface Reply{}

    public enum BenchmarkCompleted implements Reply {
        INSTANCE
    }
    public enum Ack implements Command {
        INSTANCE
    }

    public static final class NextTransaction implements Command{
        public NextTransaction(){
        }
    }


    public static Behavior<Command> create() {
        return Behaviors.setup(Coordinator::new);
    }

    private Coordinator(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RunBenchmark.class, this::onRunBenchmark)
                .onMessage(NextTransaction.class,this::onNextTransaction)
                .onMessage(Agent.AgentReply.class,this::onReceiveReply)
                .build();
    }

    private Behavior<Command> onReceiveReply(Agent.AgentReply reply){
        numVoteReceived++;
        if(reply.command == Constants.ABORT)
            voteToAbort = true;
        int currentTransaction = transactionCounter;
        if(numVoteReceived == agents.size()){
            if(voteToAbort){
                for (ActorRef<Agent.Command> agent : agents) {
                    getContext().ask(Ack.class,agent, Duration.ofSeconds(30), Agent.Abort::new,(response, throwable) ->{
                        if(response == null){
                            System.out.println("Transaction "+currentTransaction+" didn't successfully abort");
                        }
                        return null;
                    });
                }
            }else{
                for (ActorRef<Agent.Command> agent : agents) {
                    getContext().ask(Ack.class,agent, Duration.ofSeconds(30), Agent.Commit::new,(response, throwable) ->{
                        if(response == null){
                            System.out.println("Transaction "+currentTransaction+" didn't successfully commit");
                        }
                        return null;
                    });
                }
            }
            getContext().getSelf().tell(new NextTransaction());
        }
        return Behaviors.same();
    }

    private Behavior<Command> onNextTransaction(NextTransaction nt) throws IOException {
        if(transactionCounter<Constants.numTransactions){
            String[] res = tm.next();
            if(res == null){
                replyTo.tell(BenchmarkCompleted.INSTANCE);
                return Behaviors.same();
            }
            for(String i: res){
                Matcher m = Pattern.compile("\'.*?\'").matcher(i);
                ArrayList<String> results = new ArrayList<>();
                while(m.find()){
                    results.add(m.group());
                }
                int hash = (results.get(1)+results.get(2)).hashCode()%partitions.size();
                partitions.get(hash).add(i);
            }
            for(int i=0;i<agents.size();++i){
                agents.get(i).tell(new Agent.Prepare((String[])partitions.get(i).toArray(),transactionCounter));
            }
            transactionCounter++;
        }
        else {
            replyTo.tell(BenchmarkCompleted.INSTANCE);
        }
        return Behaviors.same();
    }

    private Behavior<Command> onRunBenchmark(RunBenchmark runBenchmark) throws IOException {
        replyTo = runBenchmark.replyTo;
        agents = new ArrayList<>(runBenchmark.numAgents);
        partitions = new ArrayList<>(runBenchmark.numAgents);
        for(int i=0;i<runBenchmark.numAgents;++i){
            partitions.set(i,new ArrayList<>());
        }
        tm = new TransactionManager(runBenchmark.benchmarkType,runBenchmark.minInsertions,runBenchmark.maxInsertions,runBenchmark.transactionInterval);
        getContext().getSelf().tell(new NextTransaction());
        return Behaviors.same();
    }
}