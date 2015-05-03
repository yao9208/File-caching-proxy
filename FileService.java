/* Name: Ming Yao
 * AndrewID: mingy
 * 15-640 Distributed Systems
 * Project 2 File-Caching Proxy
 */
import java.rmi.*;

public interface FileService extends Remote{
	public byte[] sendFile(long position, String path) throws RemoteException;
	public int receiveFile(String path, byte[] content, int position) throws RemoteException;
	public int fileExist(String path) throws RemoteException;
	public boolean createFile(String path) throws RemoteException;
	public int fileSize(String path) throws RemoteException;
	public boolean deleteFile(String path) throws RemoteException;
	public long Version(String path) throws RemoteException;
	public long[] metadata(String path) throws RemoteException;
}
