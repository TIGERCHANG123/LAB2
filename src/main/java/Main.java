import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Main {
    public static void main(String[] args) throws RemoteException {
        Registry registry = LocateRegistry.createRegistry(1099);
        for (int i = 0; i < 5; i ++) {
            registry.rebind("/raft/" + i, new RaftNode(i));
        }
    }
}
