/* Name: Ming Yao
 * AndrewID: mingy
 * 15-640 Distributed Systems
 * Project 2 File-Caching Proxy
 */

import java.io.*;
import java.util.HashMap;
import java.util.Map.Entry;
import java.net.MalformedURLException;
import java.rmi.*;
import java.nio.file.*;

/*
 * class proxy is to handle the remote procedure call from clients
 * and make communications with server. The main purpose is to manage
 * caching.
 */
class Proxy {
	static String address = null;
	static int port;
	static String directory = null;
	static int cacheSize;
	static LRUCache cache = null;
	static Path dirPath = null; 
	
	private static class FileHandler implements FileHandling {
		
		int startFd=3;
		HashMap<Integer, RandomAccessFile> map = new HashMap<Integer, RandomAccessFile>();
		//record fd and corresponding randomaccessfile
		HashMap<Integer, String> fileMode = new HashMap<Integer, String>();
		//record fd and corresponding open option
		HashMap<Integer, String> name = new HashMap<Integer, String>();
		//record fd and file path
		
		FileService service=null;
		
		public FileHandler() throws MalformedURLException, RemoteException, NotBoundException{
			service = (FileService) Naming.lookup
					("//"+address+":"+port+"/FileService");
		}
		
		public int open( String path, OpenOption o ) {
			Path openPath = Paths.get(path);
			path = openPath.normalize().toString();
			
			if(path.startsWith("../")){
				return Errors.EPERM;
			}
			String filePath = directory+path;
			String mode;
			File openFile = new File(filePath);
			long[] meta=null;
			int existOnServer=0;
			long serverVersion=0;
			int fileSize=0;
			try {
				//collect metadata of a file from server in one 1 RTT, to reduce latency.
				meta = service.metadata(path);
			} catch (RemoteException e2) {
				e2.printStackTrace();
			}
			existOnServer = (int) meta[0];//if a file exist on server, 1 yes, 0 no, -1 no permission
			serverVersion = meta[1];
			fileSize = (int) meta[2];
			
			if(existOnServer==-1){
				return Errors.EPERM;//the file is not visible to users
			}
			if(o.equals(OpenOption.CREATE)){
				System.err.println("create");
				if (!cache.fileExist(path)){
					try {
						if(existOnServer==1){
							//fetch the file form server
							String newpath = cache.generateReadCopyName(path);
							cache.fetchFile(path, newpath, fileSize);
							cache.setMasterCopy(path, newpath);
						}else{
							String master = cache.generateReadCopyName(path);
							File newfile = new File(directory+master);
							
							boolean success = newfile.createNewFile();
							cache.setMasterCopy(path, master);
							boolean serverSuccess = service.createFile(path);
						}
						
					} catch (IOException e) {
						e.printStackTrace();
					}
				}else{
					String masterpath = cache.getMasterCopyName(path);
					try {
						//if file is not the newest version, fetch it
						if(!isNewest(masterpath, serverVersion)){
							String master = cache.generateReadCopyName(path);
							cache.setMasterCopy(path, master);
							cache.fetchFile(path, master, fileSize);
						}
					} catch (RemoteException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					//generate private copy for write
					filePath = cache.generateCopyName(filePath);
					openFile = new File(filePath);
					String copyName = cache.generateCopyName(path);
					cache.generateCopy(cache.getMasterCopyName(path), copyName);
					path = copyName;
				} catch (IOException e) {
					e.printStackTrace();
				}
				mode = "rw";
			}else if(o.equals(OpenOption.CREATE_NEW)){
				System.err.println("create_new");
				String mastercopy = cache.getMasterCopyName(path);
				if (cache.fileExist(path)&&(!cache.shouldDelete(path))){
					System.err.println("file exist!");
					return Errors.EEXIST;
				} else{
						if(existOnServer==1){
							//as long as the file is on server, return file exist
							System.err.println("file exist!");
							return Errors.EEXIST;
						}
				}
				
				try {
					//generate master copy, and generate file on server
					String master = cache.generateReadCopyName(path);
					cache.setMasterCopy(path, master);
					File newfile = new File(directory+master);
					boolean success = newfile.createNewFile();
					boolean serverSuccess = service.createFile(path);
					cache.setMasterCopy(path, master);
					
					//generate private copy for write
					filePath = cache.generateCopyName(filePath);
					openFile = new File(filePath);
					String copyName = cache.generateCopyName(path);
					cache.generateCopy(master, copyName);
					path = copyName;
				} catch (IOException e) {
					e.printStackTrace();
				}
				mode = "rw";
			}else if(o.equals(OpenOption.READ)){
				String masterpath = cache.getMasterCopyName(path);
				if(!cache.fileExist(path)||cache.shouldDelete(masterpath)){
					try{
						if(existOnServer==0){
							System.err.println("file doesn't exist!");
							return Errors.ENOENT;
						}else{
							String master = cache.generateReadCopyName(path);
							
							cache.setMasterCopy(path, master);
							cache.fetchFile(path, master, fileSize);
							path = master;
							filePath = directory+path;
							openFile = new File(filePath);
						}
					}catch (RemoteException e1) {
						e1.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}else{
					
					try {
						if(existOnServer==0){
							System.err.println("file doesn't exist!");
							return Errors.ENOENT;
						}
						//if the master copy is not the newest.
						if(!isNewest(masterpath, serverVersion)){

							String newpath = cache.generateReadCopyName(path);
							
							cache.setMasterCopy(path, newpath);
							cache.fetchFile(path, newpath, fileSize);							
						}
					} catch (RemoteException e) {

						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}

					path = cache.getMasterCopyName(path);

					filePath = directory+path;
					openFile = new File(filePath);
				}
				mode = "r";
			}else if(o.equals(OpenOption.WRITE)){
				mode = "rw";
				if(!cache.fileExist(path)){
					try{
						if(existOnServer==0){
							System.err.println("file doesn't exist!");
							return Errors.ENOENT;
						}else{
							String newpath = cache.generateReadCopyName(path);
							cache.fetchFile(path, newpath, fileSize);
							cache.setMasterCopy(path, newpath);
						}
					}catch (RemoteException e1) {
						e1.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}else{
					String masterpath = cache.getMasterCopyName(path);
					try {
						//if file is not the newest version, fetch it
						if(!isNewest(masterpath, serverVersion)){
							String master = cache.generateReadCopyName(path);
							cache.setMasterCopy(path, master);
							cache.fetchFile(path, master, fileSize);
						}
					} catch (RemoteException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					//generate private copy
					filePath = cache.generateCopyName(filePath);
					openFile = new File(filePath);
					String copyName = cache.generateCopyName(path);
					cache.generateCopy(cache.getMasterCopyName(path), copyName);
					path = copyName;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}else{
				System.err.print("Invalid OpenOption");
				return Errors.EINVAL;
			}
			cache.use(path);
			int fildes = 0;
			for (fildes = startFd; map.containsKey(fildes); fildes++);
			try {
				if (openFile.isDirectory()){
					if (mode.equals("rw")){
						return Errors.EISDIR;
					}else{
						map.put(fildes, null);
						fileMode.put(fildes, mode);
						name.put(fildes, path);
						return fildes;
					}
				}
				RandomAccessFile file = new RandomAccessFile(filePath, mode);
				map.put(fildes, file);
				fileMode.put(fildes, mode);
				name.put(fildes, path);
				return fildes;
			} catch (FileNotFoundException e) {
				System.err.println("file not found");
				e.printStackTrace();
				return Errors.ENOENT;
				//
			}
			
		}

		public int close( int fd ) {
	
			if(!map.containsKey(fd)){
				return Errors.EBADF;
			}
			String path = name.get(fd);
			File openFile = new File(directory+name.get(fd));
			//if WRITE/CREATE/CREATE_NEW, update the file on server
			cache.useOnClose(path);
			if(fileMode.get(fd).equals("rw")){
				int success = updateFileOnServer(name.get(fd));
				cache.markFinish(path);
				try {
					cache.deleteInCache(path);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				if(success==-1){
					//if updated file size exceed cache limit?
					return 0;
				}
				
			}else{
				try {
					if(cache.shouldDelete(path)){
						cache.markFinish(path);
						cache.deleteInCache(path);
					}
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
			if (openFile.isDirectory()){
				map.remove(fd);
				fileMode.remove(fd);
				name.remove(fd);
				return 0;
			}
			RandomAccessFile file = map.get(fd);
			try {
				file.close();
				map.remove(fd);
				fileMode.remove(fd);
				name.remove(fd);
				cache.markFinish(path);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
		}

		public long write( int fd, byte[] buf ){
	
			if(!map.containsKey(fd)){
				System.err.println("bad fd");
				return Errors.EBADF;
			}
			String filePath = directory+name.get(fd);
			File f = new File(filePath);
			if (f.isDirectory()){
				System.err.println("file is a directory");
				return Errors.EISDIR;
			}
			RandomAccessFile file = map.get(fd);
			if (fileMode.get(fd).equals("r")){
				System.err.println("file can only be read");
				return Errors.EBADF;
			}
			
			long position1=0, position2=0;
			try {
				position1 = file.getFilePointer();
				synchronized (this){ 
					file.write(buf);
				}
				position2 = file.getFilePointer();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				file.close();
				map.put(fd, new RandomAccessFile(filePath, "rw"));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return position2-position1;
		}

		public long read( int fd, byte[] buf ) {
	
			if(!map.containsKey(fd)){
				System.err.println("bad fd");
				return Errors.EBADF;
			}
			File f = new File(directory+name.get(fd));
			if (f.isDirectory()){
				System.err.println("file is a directory");
				return Errors.EISDIR;
			}
			RandomAccessFile file = map.get(fd);
			int totalRead = 0;
			try {
				totalRead = file.read(buf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(totalRead==-1){
				totalRead=0;
			}
			return totalRead;
		}

		public long lseek( int fd, long pos, LseekOption o ) {

			if(!map.containsKey(fd)){
				System.err.println("bad fd");
				return Errors.EBADF;
			}
			File f = new File(directory+name.get(fd));
			if (f.isDirectory()){
				System.err.println("file is a directory");
				return Errors.EISDIR;
			}
			RandomAccessFile file = map.get(fd);
			try {
				file.seek(pos);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long position=0;
			try {
				position = file.getFilePointer();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.err.println("getFilePointer failed");
				e.printStackTrace();
			}
			return position;
		}

		public int unlink( String path ) {

			boolean success=false;
			String master = cache.getMasterCopyName(path);
			try {
				if(service.fileExist(path)==1){
						success=service.deleteFile(path);//if other is downloading?
						System.err.println("file on server deleted "+success);
				}else{
						System.err.println("file not exist");
						return Errors.ENOENT;
				}
				if (cache.fileExist(path)){
					if(!cache.isUsing(master)){
						cache.deleteInCache(master);
					}
				}
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if(success){
				return 0;
			}else{
				return -1;
			}

		}
		
		public int updateFileOnServer(String path){
			byte[] content = null;
			File file = new File(directory+path);
			int length = (int) file.length();
			int position=0;

			String serverpath = cache.originalCopyName(path);
			try {
				//send the file to server by chunk
				while (position<length) {					
					content = cache.sendFileToServer(position, path);
					if(content==null){
						break;
					}
					position = service.receiveFile(serverpath, content, position);
				}
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return 0;
		}

		public void clientdone() {

			for (Entry<Integer, RandomAccessFile> entry : map.entrySet()) {  				  
			    try {
			    	if(entry.getValue()!=null){
			    		entry.getValue().close();
			    	}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					System.err.println("close failed");
					e.printStackTrace();
				}
			}  
			//clear source of the client
			map.clear();
			fileMode.clear();
			name.clear();
			return;
		}

		public boolean isNewest(String path, long version) throws RemoteException{
			File file = new File(directory+path);
			long result = file.lastModified()-version;
			return result>=0? true:false;
		}

	}
	
	private static class FileHandlingFactory implements FileHandlingMaking {
		public FileHandling newclient() {
			try {
				return new FileHandler();
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
	}
	
	public static void main(String[] args) throws IOException, NotBoundException {
		address = args[0];
		port = Integer.parseInt(args[1]);
		directory = args[2]+"/";
		cacheSize = Integer.parseInt(args[3]);
		System.err.println("address: "+address+" port: "+port+" directory: "
		+directory+" cachesize: "+cacheSize);
	
		dirPath = Paths.get(directory);
		
		FileService svc = (FileService) Naming.lookup
				("//"+address+":"+port+"/FileService");
		cache = new LRUCache(directory, cacheSize, svc);
		(new RPCreceiver(new FileHandlingFactory())).run();
	}
}

