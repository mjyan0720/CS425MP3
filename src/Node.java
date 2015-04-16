
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

    // State control related variable
    public enum State{
        INIT, REQUEST, REQUEST_WAIT, HELD, RELEASE
    };
    private State state;
    private boolean voted;
    private Set<Integer> recvAck = new HashSet<Integer>();
    //used to store message
    private LinkedBlockingQueue<Message> messageQueue = new LinkedBlockingQueue<Message>();

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
    }

    // ******************
    // called by initializer after setup all connections
    // to start the state machine
    // ******************
    public void startStateMachine(){
        //FIXME: for test,
        //node 0 request, other wait
        if(id==0){
            stateLock.lock();
            state = State.REQUEST;
            stateLock.unlock();
        } else {
            stateLock.lock();
            state = State.RELEASE;
            stateLock.unlock();
        }
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
                while(ackCount_curr < voteSet.size()){
                    stateLock.lock();
                    ackCount_curr = recvAck.size();
                    stateLock.unlock();
                }
                //received enough acks
                //change state to held
                stateLock.lock();
                state = State.HELD;
                stateLock.unlock();
                if(option<=2){
                    String s = "CS: "+df.format(new Date())+" "+this.id+" <- ";
                    for(int id : recvAck){
                        s += " "+id+",";
                    }
                    System.out.println(s);
                }
                break;
            case HELD:
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
    public void recvMessage(Message message){

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
                    try{
                        messageQueue.put(message);
                    } catch (InterruptedException e){
                        e.printStackTrace(System.out);
                    }
                } else {
                    //reply the request
                    //set voted first then send
                    //to avoid send more than 2 ack
                    voted = true;
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
                }else{
                    voted = false;
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

}
