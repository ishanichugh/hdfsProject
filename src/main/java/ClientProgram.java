import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

public class ClientProgram {
    private static int blockSize = 8800;
    private static INameNodeInterface nameNode;
    private static int handle;

    private static boolean sendFile(String filePath) {
        try {
            FileInputStream inputStream = new FileInputStream(filePath);
            FileChannel fileChannel = inputStream.getChannel();
            ByteBuffer bb = ByteBuffer.allocate(blockSize);
            byte[] bytes;
            while (true) {
                int byteCount = fileChannel.read(bb);
                if(byteCount >= 0) {
                    bb.flip();
                    bytes = bb.array();
                    if(byteCount != blockSize) {
                        bytes = Arrays.copyOf(bytes, byteCount);
                    }
                    HDFS.AssignBlockRequest assignBlockRequest = HDFS.AssignBlockRequest
                            .newBuilder()
                            .setHandle(handle)
                            .build();
                    byte[] byteResponse = nameNode.assignBlock(assignBlockRequest.toByteArray());
                    HDFS.AssignBlockResponse assignBlockResponse = HDFS.AssignBlockResponse
                            .parseFrom(byteResponse);
                    if(assignBlockResponse.getStatus()!=200) {
                        System.out.println("No Blocks Assigned");
                        return false;
                    }
                    HDFS.BlockLocations blockLocations = assignBlockResponse.getNewBlock();
                    int blockNumber = blockLocations.getBlockNumber();
                    List<HDFS.DataNodeLocation> dataNodeLocations = blockLocations.getLocationsList();
                    for(HDFS.DataNodeLocation dataNodeLocation: dataNodeLocations.subList(0,1)) {
                        String ip = dataNodeLocation.getIp();
                        int port = dataNodeLocation.getPort();
                        Registry registry = LocateRegistry.getRegistry(ip, port);
                        IDataNodeInterface dataNode = (IDataNodeInterface) registry.lookup("dataNode");
                        HDFS.BlockLocations blockLocations1 = HDFS.BlockLocations
                                .newBuilder()
                                .setBlockNumber(blockNumber)
                                .addAllLocations(dataNodeLocations)
                                .build();
                        HDFS.WriteBlockRequest writeBlockRequest = HDFS.WriteBlockRequest
                                .newBuilder()
                                .setData(0, ByteString.copyFrom(bytes))
                                .setBlockInfo(blockLocations1)
                                .build();
                        byte[] writeResponse = dataNode.writeBlock(writeBlockRequest.toByteArray());
                        HDFS.WriteBlockResponse writeBlockResponse = HDFS.WriteBlockResponse
                                .parseFrom(writeResponse);
                        if(writeBlockResponse.getStatus()!=200) {
                            System.out.println("Not written in Data Node");
                            return false;
                        }
                        else {
                            System.out.println("Block written Successfully");
                        }
                    }
                    bb.clear();
                } else {
                    System.out.println("couldn't find");
                    break;
                }
            }
        }
        catch(IOException | NotBoundException e)
        {
            e.printStackTrace();
            return false;
        }
            return true;
    }

    static void put(String filePath) throws InvalidProtocolBufferException, RemoteException {
        HDFS.OpenFileRequest openFileRequest = HDFS.OpenFileRequest
                .newBuilder()
                .setFileName("file1")
                .setForRead(false)
                .build();
        byte[] response = nameNode.openFile(openFileRequest.toByteArray());
        HDFS.OpenFileResponse openFileResponse = HDFS.OpenFileResponse.parseFrom(response);
        if(openFileResponse.getStatus() != 200) {
            System.out.println("Couldn't open the file");
            return;
        }
        handle = openFileResponse.getHandle();
        if(!sendFile(filePath)) {
            System.out.println("Unable to send the file");
            return;
        }
    }

    static void list() {
        HDFS.ListFilesRequest listFilesRequest = HDFS.ListFilesRequest
                .newBuilder()
                .build();
        byte[] response = new byte[0];
        try {
            response = nameNode.list(listFilesRequest.toByteArray());
            HDFS.ListFilesResponse listFilesResponse = HDFS.ListFilesResponse.parseFrom(response);
            if(listFilesResponse.getStatus() != 200) {
                System.out.println("Couldn't get the list");
                return;
            }
            System.out.println("Here's the list of all the files:\n");
            String output = listFilesResponse.getFileNames(0);
            System.out.println(output);
        } catch (RemoteException | InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws RemoteException, NotBoundException, InvalidProtocolBufferException {
        System.out.println("Client Node:\n");
        Registry registry = LocateRegistry.getRegistry(1099);
        nameNode = (INameNodeInterface) registry.lookup("nameNode");
        String filePath = "/home/ishani/IdeaProjects/hdfs/src/main/resources/input.txt";
        put(filePath);
//        list();
    }
}
