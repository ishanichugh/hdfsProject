import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DataNode extends UnicastRemoteObject implements IDataNodeInterface {

    protected DataNode() throws RemoteException {
        super();
    }

    @Override
    public byte[] readBlock(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    @Override
    public byte[] writeBlock(byte[] inp) throws RemoteException {
        try {
            HDFS.WriteBlockRequest writeBlockRequest= HDFS.WriteBlockRequest
                    .parseFrom(inp);
            HDFS.BlockLocations blockLocations = writeBlockRequest.getBlockInfo();
            byte[] data = ByteString.copyFrom(writeBlockRequest.getDataList()).toByteArray();
            String path = "/home/ishani/IdeaProjects/hdfs/src/main/resources/written.txt";
            HDFS.WriteBlockResponse writeBlockResponse = HDFS.WriteBlockResponse
                    .newBuilder()
                    .setStatus(200)
                    .build();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public static void main(String[] args) throws RemoteException {
        System.out.println("Starting Data Node:");
        Registry registry;
        try {
            registry = LocateRegistry.createRegistry(2099);
        } catch (Exception e) {
            registry = LocateRegistry.getRegistry(2099);
        }
        registry.rebind("dataNode", new DataNode());
    }
}
