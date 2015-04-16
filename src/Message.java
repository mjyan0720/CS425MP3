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
}
