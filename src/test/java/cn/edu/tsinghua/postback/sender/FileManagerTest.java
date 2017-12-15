package cn.edu.tsinghua.postback.sender;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class FileManagerTest {

	public static final String POST_BACK_DIRECTORY_TEST = "src" + File.separator + "postback" + File.separator;
	public static final String UUID_PATH_TEST = POST_BACK_DIRECTORY_TEST + "uuid.txt";
	public static final String LAST_FILE_INFO_TEST = POST_BACK_DIRECTORY_TEST + "lastLocalFileList.txt";
	public static final String SENDER_FILE_PATH_TEST = POST_BACK_DIRECTORY_TEST + "data";
	public static final String SNAPSHOT_PATH_TEST = POST_BACK_DIRECTORY_TEST + "dataSnapshot";
	public Set<String> allFileList = new HashSet<>();
	public Set<String> newFileList = new HashSet<>();
	FileManager manager = FileManager.getInstance();
	
	@Before
	public void setUp() throws Exception {
		//TODO create some files
		for(int i=0; i < 5; i++) {
			String rand = String.valueOf((int)(1+Math.random()*10000)); 
			String fileName = SENDER_FILE_PATH_TEST + File.separator + rand;
			File file = new File(fileName);
			allFileList.add(fileName);
			if(!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			if(!file.exists()) {
				file.createNewFile();
			}
		}	
	}

	@After
	public void tearDown() throws Exception {
		delete(new File(SENDER_FILE_PATH_TEST));
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
	public void testBackupNowLocalFileInfo() {
		Set<String> lastFileList = new HashSet<>();
		manager.backupNowLocalFileInfo(SENDER_FILE_PATH_TEST, LAST_FILE_INFO_TEST);
		lastFileList = manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		assert(lastFileList.size() == allFileList.size()) && lastFileList.containsAll(allFileList);
	}
	
	@Test
	public void testGetLastLocalFileList() {
		Set<String> lastFileList = new HashSet<>();
		manager.backupNowLocalFileInfo(SENDER_FILE_PATH_TEST, LAST_FILE_INFO_TEST);
		lastFileList = manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		assert(lastFileList.size() == allFileList.size()) && lastFileList.containsAll(allFileList);
	}

	@Test
	public void testGetNowLocalFileList() {
		Set<String> lastFileList = new HashSet<>();
		lastFileList = manager.getNowLocalFileList(SENDER_FILE_PATH_TEST); 
		assert(lastFileList.size() == allFileList.size()) && lastFileList.containsAll(allFileList);
	}

	@Test
	public void testGetSendingFileList() throws IOException {
		Set<String> oldFileList = new HashSet<>();
		Set<String> sendingFileList = new HashSet<>();
		manager.backupNowLocalFileInfo(SENDER_FILE_PATH_TEST, LAST_FILE_INFO_TEST);
		//TODO create some new files
		for(int i=0; i < 5; i++) {
			String rand = String.valueOf((int)(1+Math.random()*10000)); 
			String fileName = SENDER_FILE_PATH_TEST + File.separator + rand;
			File file = new File(fileName);
			allFileList.add(fileName);
			newFileList.add(fileName);
			if(!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			if(!file.exists()) {
				file.createNewFile();
			}
		}
		oldFileList = manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		sendingFileList = manager.getSendingFileList(oldFileList, allFileList);
		assert(sendingFileList.size() == newFileList.size()) && sendingFileList.containsAll(newFileList);		
	}
}
