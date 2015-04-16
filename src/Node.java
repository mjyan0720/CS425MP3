
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.text.*;
import java.util.concurrent.*;
// ***************************************
//
// class Node
// implement the state machine
// init -> requrest -> hold -> wait
//
// *************************************


public class Node implements Runnable{

    private int cs_int;
    private int next_req;
    private int option;

    private int id;

    private long timeout;
    static private int delayUnit = 500;//ms

    // State control related variable
    public enum State{
        INIT, REQUEST, REQUEST_WAIT, HELD, RELEASE, DEADLOCK_RESOLVE 
    };
    private State state;
    private boolean voted;
    private int voteId;
    private Set<Integer> recvAck = new HashSet<Integer>();
    //used to store message
    private PriorityQueue<Message> messageQueue = new PriorityQueue<Message>(
            Initializer.N, new messageComparator());

    public class messageComparator implements Comparator<Message>{
        public int compare(Message m1, Message m2){
            if(m1.srcId<m2.srcId)
                return -1;
            else if(m1.srcId==m2.srcId)
                return 0;
            else return 1;
        }
    }

    //state, voted, ackCount and the queue are all considered
    //as node state
    //access any of them requires to aquire the lock
    private Lock  stateLock;

    //vote nodes id and communicate channel
    private Map<Integer, Node> voteSet = new HashMap<Integer, Node>();

    //used for print
    public DateFormat df = new SimpleDateFormat("HH:mm:ss.SSSS");

    // ****************
    // constructor
    // ****************
    public Node(int cs_int, int next_req, int option,
            int id, State state){
        this.cs_int = cs_int;
        this.next_req = next_req;
        this.option = option;
        this.id = id;
        this.state = state;
        this.stateLock = new ReentrantLock();
        this.voted = false;
        this.voteId = -1;
    }

    public Node(int cs_int, int next_req, int option, int id){
        this(cs_int, next_req, option, id, State.INIT);    
    }

    public int getId(){
        return id;
    }

    public State getState(){
        return state;
    }

    public Set<Integer> getVoteSet(){
        return new TreeSet<Integer>(voteSet.keySet());
    }


    // *******************
    // set vote set
    // called by initializer
    // also set communicate channels between vote set nodes
    // *******************
    public void setVoteSet(Set<Node> vs){
        for(Node n : vs){
            voteSet.put(n.getId(), n);
        }
        //set the timeout as waiting for all other voters in
        //the voteset to get the critical section(or assign other
        //processors once).
        //It's not the most pessimistic case, therefore node will
        //abort even though there's not deadlock.
        this.timeout = (voteSet.size()-1)*cs_int*1000;//ms
    }

    // ******************
    // called by initializer after setup all connections
    // to start the state machine
    // ******************
    public void startStateMachine(){
            stateLock.lock();
            state = State.REQUEST;
            stateLock.unlock();
    }

    // *******************
    // main function to implement state machine
    // ******************
    public void run(){
 
        while(true){

        stateLock.lock();
        State state_curr = state;
        stateLock.unlock();

        if(option==2)
            System.out.println(this.id+" is in state "+this.state+" at "
                    +df.format(new Date()));

        switch(state_curr){
            case REQUEST:
                stateLock.lock();
                recvAck.clear();
                state = State.REQUEST_WAIT;
                stateLock.unlock();
                sendRequestMessage();
                break;
            case REQUEST_WAIT:
                //wait for replies from all voters
                int ackCount_curr = 0;
                long timestamp_wait_start = System.currentTimeMillis();
                long timestamp_curr = System.currentTimeMillis();
                while(ackCount_curr < voteSet.size()
                        && (timestamp_curr-timestamp_wait_start)< this.timeout){
                    stateLock.lock();
                    ackCount_curr = recvAck.size();
                    stateLock.unlock();
                    timestamp_curr = System.currentTimeMillis();
                }
                //received enough acks
                //change state to held
                stateLock.lock();
                if(ackCount_curr<voteSet.size()){
                    state = State.DEADLOCK_RESOLVE;
                } else {
                    state = State.HELD;
                }
                stateLock.unlock();
                break;
            case DEADLOCK_RESOLVE:
                //if a deadlock is detected, send giveup message
                //in order to avoid livelock, give preference to 
                //processor with smaller id
                boolean winner = true;
                stateLock.lock();
                if(voteId>=this.id){
                    //check the queue to see any smaller
                    //requestor id, if exists, not winner
                    //this time
                    for(Message m : messageQueue){
                        if(m.srcId<this.id){
                            winner=false;
                            break;
                        }
                    }
                }else{
                    //already voted for a smaller id
                    //not winner this time
                    winner = false;
                }
                stateLock.unlock();
                // -- 1. send give up messages
                if(winner==false){
                    sendGiveupMessage();
                // -- 2. sleep for a random length of time
                //Random rand = new Random(System.currentTimeMillis());
                //sleep for 0~0.5s, increase resolution
                //int random_num = rand.nextInt(preRequestMaxTime/10)*10;
                int delay = this.id * delayUnit;
                    try{
                        Thread.sleep(delay);
                    }catch(InterruptedException e){
                        e.printStackTrace(System.out);
                    }
                }
                stateLock.lock();
                if(winner==false){
                    state = State.REQUEST;
                } else {
                    state = State.REQUEST_WAIT;
                }
                stateLock.unlock();
                break;
            case HELD:
               if(option<=2){
                    String s = "CS: "+df.format(new Date())+" "+this.id+" <- ";
                    for(int id : recvAck){
                        s += " "+id+",";
                    }
                    System.out.println(s);
                }
                //hold the critical section for cs_int(seconds)
                try{
                    Thread.sleep(cs_int*1000);
                }catch(InterruptedException e){
                    e.printStackTrace(System.out);
                }
                //multicast to voters to release
                stateLock.lock();
                state = State.RELEASE;
                stateLock.unlock();
                sendReleaseMessage();
                break;
            case RELEASE:
                try{
                    Thread.sleep(next_req*1000);
                }catch(InterruptedException e){
                    e.printStackTrace(System.out);
                }
                stateLock.lock();
                state = State.REQUEST;
                stateLock.unlock();
                break;
            default:
                if(option==2)
                    System.out.println("Invalid State");
                break;
        }

        }
    }


    // **********************************
    // called by other threads
    // used as communication channel
    // **********************************
    public synchronized void recvMessage(Message message){

        Date date = new Date(message.timestamp);
        if(option>0){
            System.out.println("M : "+df.format(date)+" "+this.id+" <- "+
                    message.srcId+" "+message.type);
        }

        switch(message.type){
            case REQUEST:{
                Message replyMessage = null;
                stateLock.lock();//Aquire Lock
                //respond with regarding to state
                if(state==State.HELD || voted==true){
                    //queue the request
                    messageQueue.add(message);
                } else {
                    //reply the request
                    //set voted first then send
                    //to avoid send more than 2 ack
                    voted = true;
                    voteId = message.srcId;
                    replyMessage = new Message(this.id, message.srcId,
                            System.currentTimeMillis(), Message.Type.REQUEST_ACK);
                }
                stateLock.unlock();//Release Lock
                if(replyMessage!=null)
                    voteSet.get(message.srcId).recvMessage(replyMessage);
                break;
            }
            case RELEASE:{
                Message nextMessage = null;
                stateLock.lock();//Acquire Lock
                if(messageQueue.size()!=0){
                    voted = true;
                    nextMessage = messageQueue.poll();
                    voteId = nextMessage.srcId;
                }else{
                    voted = false;
                    voteId = -1;
                }
                stateLock.unlock();//Release Lock
                if(nextMessage!=null){
                    Message replyMessage = new Message(this.id, nextMessage.srcId,
                            System.currentTimeMillis(), Message.Type.REQUEST_ACK);
                    voteSet.get(nextMessage.srcId).recvMessage(replyMessage);
                }
                break;
            }
            case GIVEUP:
                //delete corresponding request message
                stateLock.lock();//Aqurie Lock
                if(voteId == message.srcId){
                    voted = false;
                    voteId = -1;
                }else{//delete it from the queue
                    Message recvMessage = new Message(message);
                    recvMessage.type = Message.Type.REQUEST;
                    boolean res = messageQueue.remove(recvMessage);
                    assert res == true : "giveup can't find corresponding message in messageQueue";
                }
                stateLock.unlock();//Release Lock
                break;
            case REQUEST_ACK:
                stateLock.lock();//Acquire Lock
                recvAck.add(message.srcId);
                stateLock.unlock();//Release Lock
                break;
            default:
                if(option==2)
                    System.out.println("Unrecognized message type.");
                break;
        }

    }

    //send request message to its vote set
    private void sendRequestMessage(){
       long timestamp = System.currentTimeMillis();
       int srcId = this.id;
       //send request message
       for(Node n : voteSet.values()){
           int destId = n.getId();
           Node destNode = n;
           Message m = new Message(srcId, destId, timestamp, Message.Type.REQUEST);
           n.recvMessage(m);
       }
    }

    //send request message to its vote set
    private void sendReleaseMessage(){
       long timestamp = System.currentTimeMillis();
       int srcId = this.id;
       //send request message
       for(Node n : voteSet.values()){
           int destId = n.getId();
           Node destNode = n;
           Message m = new Message(srcId, destId, timestamp, Message.Type.RELEASE);
           n.recvMessage(m);
       }
    }

    private void sendGiveupMessage(){
       for(Node n : voteSet.values()){
          //send give up to those haven't respond
          Message m = new Message(this.id, n.getId(),
              System.currentTimeMillis(), Message.Type.GIVEUP);
         if(option==2){
           System.out.println(this.id+" -> "+m.destId+" "+m.type);
         }
         n.recvMessage(m);
       }

    }
}
