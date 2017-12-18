package cn.edu.tsinghua.postback.sender;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import cn.edu.tsinghua.postback.receiver.*;

public class TransferDataTest {

	public static final String POST_BACK_DIRECTORY_TEST = "src" + File.separator + "postback" + File.separator;
	public static final String UUID_PATH_TEST = POST_BACK_DIRECTORY_TEST + "uuid.txt";
	public static final String LAST_FILE_INFO_TEST = POST_BACK_DIRECTORY_TEST + "lastLocalFileList.txt";
	public static final String SENDER_FILE_PATH_TEST = POST_BACK_DIRECTORY_TEST + "data";
	public static final String SNAPSHOT_PATH_TEST = POST_BACK_DIRECTORY_TEST + "dataSnapshot";
	public static final String SERVER_IP_TEST = "127.0.0.1";
	public static final int SERVER_PORT_TEST = 5555;
	ServerManager serverManager = ServerManager.getInstance();
	TransferData transferData = TransferData.getInstance();
	ServiceImp serviceImp = new ServiceImp();
	FileManager manager = FileManager.getInstance();
	
	@Before
	public void setUp() throws Exception {
		File file =new File(LAST_FILE_INFO_TEST);
		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		if (!file.exists()) {
			file.createNewFile();
		}
		file =new File(SENDER_FILE_PATH_TEST);
		if (!file.exists()) {
			file.mkdirs();
		}
	}

	@After
	public void tearDown() throws Exception {
		serverManager.closeServer();
		String POST_BACK_DIRECTORY = "src" + File.separator + "postback";
		delete(new File(POST_BACK_DIRECTORY));
		new File(POST_BACK_DIRECTORY).delete();
	}

	public void delete(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} 
		else{
			File[] files = file.listFiles();
			for (File f : files) {
				delete(f);        
				f.delete();       
			}
		}		
	}
	
	@Test
	public void testTransferUUID() {
		String uuidOfSender;
		String uuidOfReceiver;
		serverManager.startServer(SERVER_PORT_TEST);
		transferData.connection(SERVER_IP_TEST, SERVER_PORT_TEST);
		
		//generate uuid and write it to file
		uuidOfReceiver = transferData.transferUUID(UUID_PATH_TEST);
		uuidOfSender = transferData.uuid;
		assert(uuidOfReceiver.equals(uuidOfSender));
		
		//read uuid from file
		serverManager.closeServer();
		serverManager.startServer(SERVER_PORT_TEST);
		transferData.connection(SERVER_IP_TEST, SERVER_PORT_TEST);
		uuidOfReceiver = transferData.transferUUID(UUID_PATH_TEST);
		uuidOfSender = transferData.uuid;
		assert(uuidOfReceiver.equals(uuidOfSender));
	}

	@Test
	public void testFileSnapshot() throws Exception {
 		Set<String> sendingFileList = new HashSet<>();
 		Set<String> lastlocalList = new HashSet<>();
 		Set<String> fileList = new HashSet<>();
		Random r = new Random(0);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		lastlocalList = manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		fileList = manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
 		sendingFileList = manager.getSendingFileList(lastlocalList, fileList);
 		transferData.fileSnapshot(sendingFileList, SNAPSHOT_PATH_TEST);
 		//compare all md5 of source files and snapshot files
 		for(String filePath:sendingFileList)
 		{
 			String md5OfSource = getMD5(new File(filePath));
 			String newPath = SNAPSHOT_PATH_TEST + File.separator + filePath;
 			String md5OfSnapshot = getMD5(new File(newPath));
 			assert(md5OfSource.equals(md5OfSnapshot));
 		}
	}

	public String getMD5(File file) throws Exception
	{
		FileInputStream fis = new FileInputStream(file);
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] buffer = new byte[1024];
		int length = -1;
		while ((length = fis.read(buffer, 0, 1024)) != -1) {
			md.update(buffer, 0, length);
		}
		String md5 = (new BigInteger(1, md.digest())).toString(16);
		fis.close();
		return md5;
	}
	
	@Test
	public void testStartSending() throws Exception {
		Set<String> sendingFileList = new HashSet<>();
 		Set<String> lastlocalList = new HashSet<>();
 		Set<String> fileList = new HashSet<>();
 		
 		serverManager.startServer(SERVER_PORT_TEST);
		transferData.connection(SERVER_IP_TEST, SERVER_PORT_TEST);
		transferData.transferUUID(UUID_PATH_TEST);
		
		Random r = new Random(0);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		lastlocalList = manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		fileList = manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
 		sendingFileList = manager.getSendingFileList(lastlocalList, fileList);
 		transferData.fileSnapshot(sendingFileList, SNAPSHOT_PATH_TEST);
 		transferData.startSending(sendingFileList, SNAPSHOT_PATH_TEST);
 		//compare all md5 of source files and server files
 		for(String filePath:sendingFileList)
 		{
 			String md5OfSource = getMD5(new File(filePath));
 			String newPath = POST_BACK_DIRECTORY_TEST + transferData.uuid + File.pathSeparator + filePath;
 			String md5OfSnapshot = getMD5(new File(newPath));
 			assert(md5OfSource.equals(md5OfSnapshot));
 		}
	}

	@Test
	public void testAfterSending() throws Exception {
		Set<String> sendingFileList = new HashSet<>();
 		Set<String> lastlocalList = new HashSet<>();
 		Set<String> fileList = new HashSet<>();
 		
 		serverManager.startServer(SERVER_PORT_TEST);
		transferData.connection(SERVER_IP_TEST, SERVER_PORT_TEST);
		transferData.transferUUID(UUID_PATH_TEST);
		
		Random r = new Random(0);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		lastlocalList = manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		fileList = manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
 		sendingFileList = manager.getSendingFileList(lastlocalList, fileList);
 		transferData.fileSnapshot(sendingFileList, SNAPSHOT_PATH_TEST);
 		transferData.startSending(sendingFileList, SNAPSHOT_PATH_TEST);
 		transferData.afterSending(SNAPSHOT_PATH_TEST);
 		assert(new File(SNAPSHOT_PATH_TEST).list().length==0);
	}

}
