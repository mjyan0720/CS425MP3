import java.io.*;
import java.util.*;

public class Message{
    public int srcId;
    public int destId;
    public long timestamp;
    public Type   type;

    public enum Type{
        REQUEST, RELEASE, GIVEUP,
        REQUEST_ACK
    };

    public Message(int srcId, int destId, long timestamp,
            Type type){
        this.srcId = srcId;
        this.destId = destId;
        this.timestamp = timestamp;
        this.type = type;
    }

    public Message(Message m){
        this.srcId = m.srcId;
        this.destId = m.destId;
        this.timestamp = m.timestamp;
        this.type = m.type;
    }

    public boolean equals(Object o){
        if(o instanceof Message){
            Message m = (Message)o;
            if(m.srcId==this.srcId && m.destId==this.destId
                    && m.type==this.type){
                return true;
            }
        }
        return false;
    }
}
