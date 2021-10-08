import com.google.protobuf.InvalidProtocolBufferException;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

public class NameNode extends UnicastRemoteObject implements INameNodeInterface {
    private int totalFiles;
    private HashMap<String, ArrayList<Integer>> filesStored;
    protected NameNode() throws RemoteException {
        super();
        totalFiles = 0;
    }

    @Override
    public byte[] openFile(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        HDFS.OpenFileRequest openFileRequest = HDFS.OpenFileRequest
                .parseFrom(inp);
        String fileName = openFileRequest.getFileName();
        int handle = totalFiles+1;
        filesStored.put(fileName, null);
        boolean read = openFileRequest.getForRead();
        HDFS.OpenFileResponse openFileResponse = HDFS.OpenFileResponse
                .newBuilder()
                .setStatus(200)
                .setHandle(handle)
                .build();
        return openFileResponse.toByteArray();
    }

    @Override
    public byte[] closeFile(byte[] inp) throws RemoteException {
        try {
            HDFS.CloseFileRequest closeFileRequest = HDFS.CloseFileRequest
                    .parseFrom(inp);
            int handle = closeFileRequest.getHandle();
            // close file
            HDFS.CloseFileResponse closeFileResponse =  HDFS.CloseFileResponse
                    .newBuilder()
                    .setStatus(200)
                    .build();
            return closeFileResponse.toByteArray();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public byte[] getBlockLocations(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    @Override
    public byte[] assignBlock(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    @Override
    public byte[] list(byte[] inp) throws RemoteException {
        try {
            HDFS.ListFilesRequest listFilesRequest = HDFS.ListFilesRequest
                    .parseFrom(inp);
            HDFS.ListFilesResponse listFilesResponse = HDFS.ListFilesResponse
                    .newBuilder()
                    .setStatus(200)
                    .build();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public byte[] blockReport(byte[] inp) throws RemoteException {
        try {
            HDFS.AssignBlockRequest assignBlockRequest = HDFS.AssignBlockRequest
                    .parseFrom(inp);
            Integer handle = assignBlockRequest
                    .getHandle();
            String fileName = "file" + Integer.toString(handle);
            ArrayList<HDFS.DataNodeLocation>dataNodeLocations = new ArrayList<>();
            HDFS.DataNodeLocation dataNodeLocation= HDFS.DataNodeLocation
                    .newBuilder()
                    .setIp("127.0.0.1")
                    .setPort(2099)
                    .build();
            dataNodeLocations.add(dataNodeLocation);
            HDFS.AssignBlockResponse assignBlockResponse = HDFS.AssignBlockResponse
                    .newBuilder()
                    .setStatus(200)
                    .setNewBlock(HDFS.BlockLocations.newBuilder()
                            .setBlockNumber(1)
                            .addAllLocations(dataNodeLocations)
                            .build())
                    .build();
            return assignBlockResponse.toByteArray();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public byte[] heartBeat(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public static void main(String[] args) throws RemoteException {
        System.out.println("Starting Name Node:\n");
        Registry registry;
        try {
            registry = LocateRegistry.createRegistry(1099);
        } catch (RemoteException e) {
            registry = LocateRegistry.getRegistry(1099);
        }
        registry.rebind("nameNode", new NameNode());
    }
}
