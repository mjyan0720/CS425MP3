
import java.io.*;
import java.util.*;

//************************************************
// Meakawa's Algorithm
//
// Main class used to read in argument
// and spawn N connected nodes
//
// ************************************************

public class Initializer{

    int cs_int;//time spent in critical section
    int next_req;//time between exit critical section and
                 //start next request
    int tot_exec_time;//total execution time, unit: s
    int option;//log format
    int N;//number of nodes

    Vector<Node> nodes = new Vector<Node>();
    Vector<Thread> threads = new Vector<Thread>();

    public Initializer(int cs_int, int next_req, 
            int tot_exec_time, int option, int N){

        System.out.println("Start Initializer.");
        System.out.println("cs_int="+cs_int+"; next_req="+next_req
                +"; tot_exec_time="+tot_exec_time+"; option="+option
                +"; N="+N);

        this.cs_int = cs_int;
        this.next_req = next_req;
        this.tot_exec_time = tot_exec_time;
        this.option = option;
        this.N = N;
        double sqrtN = Math.sqrt(N);
        //require N = x*x;
        assert (Math.ceil(sqrtN) == Math.floor(sqrtN));

        //create nodes and set up connections
        createNodes();

        //once connections all set up
        //move all nodes from init state to request state
        startNodes();

        //sleep for total execution time, when reach
        //kill all thread and exit
        try{
            Thread.sleep(tot_exec_time*1000);
        } catch(InterruptedException e){
            e.printStackTrace(System.out);
        }
        killNodes();
        System.exit(0);
    }

    public Initializer(int cs_int, int next_req, 
            int tot_exec_time, int option){
        this(cs_int, next_req, tot_exec_time, option, 9);
    }


    private void createNodes(){

        System.out.println("Create "+N+" nodes...");
        for(int i=0; i<N; i++){
            nodes.add(new Node(cs_int, next_req, option, i));
        }

        //assign vote set
        int sqrtN = (int) Math.sqrt(N);
        for(int i=0; i<N; i++){
            int col = i % sqrtN;
            int row = i / sqrtN;
            Set<Node> voteSet_i = new HashSet<Node>();
            for(int j=0; j<sqrtN; j++){
                //add row
                voteSet_i.add(nodes.get(row*sqrtN+j));
                //add column
                voteSet_i.add(nodes.get(j*sqrtN+col));
            }
            assert voteSet_i.size() == (2*sqrtN-1);
            nodes.get(i).setVoteSet(voteSet_i);
        }

    }

    private void startNodes(){

        for(Node n : nodes){
            n.startStateMachine();
            threads.add(new Thread(n));
        }
        for(Thread t : threads){
            t.start();
        }
    }

    private void killNodes(){
        //TODO: really need to kill threads??
    }


    public static void main(String [] args){
        // input: cs_int, next_req, tot_exec_time, option

        int cs_int = Integer.parseInt(args[0]);
        int next_req = Integer.parseInt(args[1]);
        int tot_exec_time = Integer.parseInt(args[2]);
        int option = Integer.parseInt(args[3]);

        new Initializer(cs_int, next_req, tot_exec_time, option);

    }
}
