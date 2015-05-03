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
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedList;
import java.rmi.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/*
 * Use a linkedlist to implement the LRU replacement policy
 * managed by proxy class
 */

public class LRUCache {
	String directory;
	int sizeLimit;
	int size;
	FileService service;
	LinkedList<cacheElement> list;
	HashMap<String, Integer> map = new HashMap<String, Integer>(); 
	HashMap<String, Integer> readCopyVersion = new HashMap<String, Integer>();
	HashMap<String, String> masterCopy = new HashMap<String, String>();
	public LRUCache(String directory, int sizeLimit, FileService service){
		this.directory = directory;
		this.sizeLimit = sizeLimit;
		this.service = service;
		this.size=0;
		list = new LinkedList<cacheElement>();
	}
	
	//fetch a file from server
	public boolean fetchFile(String path, String copyPath, int fileLength) throws IOException{
		if(fileLength>sizeLimit){
			System.err.println("file size greater than cache limit");
			return false;
		}
		while((sizeLimit-size)<fileLength){
			evict();
		}
		addElement(copyPath, fileLength);
		writeFile(path, copyPath);
		return true;
	}
	
	//add a new eleament to the head of list
	public void addElement(String path, int fileSize){
		File file = new File(directory+path);
		cacheElement e = new cacheElement(path, file, 0);
		list.addFirst(e);
		size += fileSize;
	}
	
	//when open or close, update the list
	public void use(String path){		
		for(int i=0; i<list.size(); i++){
			if(list.get(i).path.equals(path)){
				cacheElement e = list.remove(i);
				e.using++;
				list.addFirst(e);
				return;
			}
		}
	}
	public void useOnClose(String path){
		for(int i=0; i<list.size(); i++){
			if(list.get(i).path.equals(path)){
				cacheElement e = list.remove(i);
				list.addFirst(e);
				return;
			}
		}
	}
	
	public synchronized void markFinish(String path){
		for(int i=0; i<list.size(); i++){
			if(list.get(i).path.equals(path)){
				cacheElement e = list.get(i);
				e.using--;
				return;
			}
		}
	}
	
	public boolean evict(){
		if(list.isEmpty()){
			System.err.println("evict error: list is empty");
			return false;
		}
		cacheElement e=null;
		int i=list.size();
		for(i=list.size()-1; i>=0; i--){
			if(list.get(i).using==0){
				e = list.remove(i);
				break;
			}
		}
		if(i<0){
			System.err.println("all files are in use");
			return false;
		}
		File file = e.file;
		size -= file.length();
		boolean success = file.delete();
		return success;
		
	}
	
	//delete a file just in cache
	public void deleteInCache(String path) throws RemoteException{
		File file = new File(directory+path);
		int fileSize = (int) file.length();
		for(int i=0; i<list.size(); i++){
			if(list.get(i).path.equals(path)){
				if(list.get(i).using==0){
					list.remove(i);
					break;
				}else{
					list.get(i).delete=true;
					return;
				}				
			}
		}
		boolean success = file.delete();
		size -= fileSize;
	}
	
	//generate a private copy's file name
	public synchronized String generateCopyName(String path){
		int version = 1;
		if(!map.containsKey(path)){
			map.put(path, 1);
		}else{
			version = map.get(path)+1;
			map.put(path, version);
		}
		return path+".tmp"+version;
	}
	
	//generate a read copy's name
	public synchronized String generateReadCopyName(String path){
		int version = 1;
		if(!readCopyVersion.containsKey(path)){
			readCopyVersion.put(path, 1);
		}else{
			version = readCopyVersion.get(path)+1;
			readCopyVersion.put(path, version);
		}
		return path+".v"+version+".read";
	}
	
	//recover a file name from its read copy's name
	public String getOriginalReadCopyName(String path){
		if(path.endsWith(".read")){
			int index = path.substring(0, path.lastIndexOf(".")).lastIndexOf(".");
			return path.substring(0, index);
		}
		return path;
	}
	
	//recover a file name from its write copy's name
	public String originalCopyName(String path){
		int index = path.lastIndexOf('.');
		return path.substring(0, index);
	}
	
	//write a file's content to a byte array to be send to server, read by chunk
	public void writeFile(String path, String cachePath) throws IOException{
		byte[] chunck = null;
		int chunksize=1000007;
		boolean finish = false;
		
		RandomAccessFile fout=null;
		File file = new File(directory+cachePath);
		file.getParentFile().mkdirs();
		if(!file.exists()){
			file.createNewFile();
		}
		long position=0;
		try {
			if(!file.exists()){
				file.createNewFile();
			}
			fout = new RandomAccessFile(directory+cachePath, "rw");
				
			while (!finish) {
				fout = new RandomAccessFile(directory+cachePath, "rw");
				chunck = service.sendFile(position, path);
				fout.seek(position);
				fout.write(chunck);
				position = fout.getFilePointer();
				fout.close();
				finish = chunck.length < chunksize || chunck == null ? true:false;
			}
			fout.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	//copy form master copy to private copy
	public boolean generateCopy(String originalFile, String copyName) throws IOException{
		File source = new File(directory+originalFile);
		long fileLength = source.length();
		while((sizeLimit-size)<fileLength){
			evict();
		}
		File dest = new File(directory+copyName);
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
			try {
			inputChannel = new FileInputStream(source).getChannel();
			outputChannel = new FileOutputStream(dest).getChannel();
			outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
		} finally {
			inputChannel.close();
			outputChannel.close();
		}
		addElement(copyName, (int) fileLength);
		return true;
	}

	public byte[] sendFileToServer(long position, String path){
		int chuncksize=1000007;
		
		File file = new File(directory+path);
		int fileLength = (int) file.length();
		if(position>=fileLength){
			return null;
		}
		boolean finish=false;
		byte[] fileContent = new byte[(int) Math.min(chuncksize, fileLength-position)];
		int read=0;
		RandomAccessFile fin=null;
		if(fileLength>sizeLimit){
			System.err.println("updated file size excceed cache limit");
		}
		while((sizeLimit-size)<fileLength){
			boolean success = evict();
			if(!success){
				return null;
			}
		}

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
		return fileContent;
	}
	
	//a new master copy comes, update it
	public void setMasterCopy(String path, String newCopyPath) throws RemoteException{
		if(masterCopy.containsKey(path)){
			if(!isUsing(masterCopy.get(path))){
				String formerMaster = masterCopy.get(path);
				File file = new File(directory+masterCopy.get(path));
				int fileSize = (int) file.length();
				for(int i=0; i<list.size(); i++){
					if(list.get(i).path.equals(formerMaster)){
						if(list.get(i).using==0){
							list.remove(i);
							size -= fileSize;
							boolean success=file.delete();
							break;
						}else{
							break;
						}				
					}
				}
				
			}
		}
		masterCopy.put(path, newCopyPath);

	}
	public String getMasterCopyName(String path){
		if(!masterCopy.containsKey(path)){
			return null;
		}
		return masterCopy.get(path);
	}
	public boolean fileExist(String path){
		if(masterCopy.containsKey(path)){
			File file = new File(directory+masterCopy.get(path));
			if(file.exists()){
				return true;
			}
		}
		return false;
	}

	public boolean isUsing(String path){

		for(int i=0; i<list.size(); i++){
			if(path.equals(list.get(i).path)){
				boolean result = list.get(i).using>0? true:false;
				if(result){
					list.get(i).delete=true;//delete after close
				}
				return result;
			}
		}
		return false;
	}
	
	//we can't delete a file when someoen using it, instead we mark it as should be deleted on close
	public boolean shouldDelete(String path){
		if(path==null){
			return false;
		}
		for(int i=0; i<list.size(); i++){
			if(path.equals(list.get(i).path)){

				return list.get(i).delete;
			}
		}
		return false;
	}

}
class cacheElement{
	String path;
	File file;
	int using;
	boolean delete;
	public cacheElement(String path, File file, int using){
		this.path = path;
		this.file = file;
		this.using = using;
	}
}