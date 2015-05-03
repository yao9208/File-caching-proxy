/* Name: Ming Yao
 * AndrewID: mingy
 * 15-640 Distributed Systems
 * Project 2 File-Caching Proxy
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.*;
import java.nio.file.*;

/*
 * the server class is to manage files on server and communicate with proxy
 * use java rmi to process remote calls
 */
public class Server extends UnicastRemoteObject implements FileService{
	static String directory;
	static Path dirPath;
	static String absDirPath;


	protected Server() throws RemoteException {
		super();
	}

	//read a local file into a byte array to be send to proxy, read by chunk
	public byte[] sendFile(long position, String path) throws RemoteException {
		File file = new File(directory+path);
		int fileLength = (int) file.length();
		if(position>=fileLength-1){
			return null;
		}
		int chunksize=1000007;
		boolean finish=false;
		byte[] fileContent = new byte[(int) Math.min(chunksize, fileLength-position)];
		int read=0;

		RandomAccessFile fin=null;
		try {
			fin = new RandomAccessFile(file, "r");
			fin.seek(position);
			read = fin.read(fileContent);

			fin.close();
		} catch (FileNotFoundException e) {	
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(read<chunksize){
			finish=true;
		}
		
		return fileContent;
	}

	//receive file from proxy, write into a local file, return number of write bytes
	public int receiveFile(String path, byte[] content, int position) throws RemoteException {
		RandomAccessFile randomFile = null;
		try {
			randomFile = new RandomAccessFile(directory+path, "rw");
			if(position==0){
				randomFile.setLength(0);
			}
			randomFile.seek(position);
			randomFile.write(content);
			position = (int) randomFile.getFilePointer();
			randomFile.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return position;
		
	}
		
	// to see whether a file is on the server
	public int fileExist(String path) throws RemoteException {
		File file = new File(directory+path);
		Path filePath = Paths.get(directory+path).toAbsolutePath().normalize();

		if(!filePath.startsWith(dirPath)){
			return -1;
		}
		if(file.exists()){
			return 1;
		}
		return 0;
	}
	
	@Override
	public boolean createFile(String path) throws RemoteException {
		File file = new File(path);
		boolean success = false;
		try {
			success = file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return success;
	}

	public static void main ( String args[] ) throws Exception
    {
		int port = Integer.parseInt(args[0]);
		directory = args[1]+"/";
        Server svr = new Server();
        Registry registry = null;
        dirPath = Paths.get(directory).toAbsolutePath().normalize();
        absDirPath = dirPath.toString();


        try {
			//create the RMI registry if it doesn't exist.
			LocateRegistry.createRegistry(port);
		}
		catch(RemoteException e) {
			System.err.println("Failed to create the RMI registry " + e);
		}
        Naming.rebind ("//127.0.0.1:"+port+"/FileService", svr);

        System.err.println ("Service bound to "+"//127.0.0.1:"+port+"/FileService");
    }

	@Override
	public int fileSize(String path) throws RemoteException{
		File file = new File(directory+path);
		return (int) file.length();
	}

	@Override
	public boolean deleteFile(String path) throws RemoteException {
		File file = new File(directory+path);		
		return file.delete();
	}
	
	@Override
	public long Version(String path) throws RemoteException{
		File file = new File(directory+path);
		return file.lastModified();
	
	}
	
	//collect metada information of a file to reduce latency
	public long[] metadata(String path) throws RemoteException{
		long[] result = new long[3];
		result[0] = fileExist(path);
		result[1] = Version(path);
		result[2] = fileSize(path);
		return result;
	}


}
