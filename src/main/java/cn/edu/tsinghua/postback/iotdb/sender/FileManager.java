package cn.edu.tsinghua.postback.iotdb.sender;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.postback.Config;

public class FileManager {
	
	private Set<String> sendingFiles;
	private Set<String> lastLocalFiles;
	private Set<String> nowLocalFiles;

	private static final Logger LOGGER = LoggerFactory.getLogger(FileManager.class);
	private static class FileManagerHolder{
		private static final FileManager INSTANCE = new FileManager();
	}
	private FileManager() {}
	

	public static final FileManager getInstance() {
		return FileManagerHolder.INSTANCE;
	}

	public void getSendingFileList() {
		Set<String> oldFiles = lastLocalFiles;
		Set<String> newFiles = nowLocalFiles;
		Set<String> fileList = new HashSet<>();
		for(String newFile : newFiles) {
			if(!oldFiles.contains(newFile)) {
				fileList.add(newFile);
			}
		}
		sendingFiles = fileList;
	}

	public void getLastLocalFileList(String path)  {
		Set<String> fileList = new HashSet<>();
		File file = new File(path);
		try {
			if (!file.exists()) {
				file.createNewFile();
			} else {
				BufferedReader bf = null;
				try {
					bf = new BufferedReader(new FileReader(file));
					String fileName = null;
					while ((fileName = bf.readLine()) != null) {
						fileList.add(fileName);
					}
					bf.close();
				} catch (IOException e) {
					LOGGER.error("IoTDB post back sender: cannot get last pass local file list when reading file {} because {}", Config.LAST_FILE_INFO, e);
				} finally {
					if(bf != null) {
						bf.close();
					}
				}
			}
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot get last pass local file list because {}", e);
		}
		lastLocalFiles = fileList;
	}

	public void getNowLocalFileList(String path) {
		
		Set<String> fileList = new HashSet<>();
		try (Stream<Path> filePathStream = Files.walk(Paths.get(path))) {
			filePathStream.filter(Files::isRegularFile).forEach(filePath -> {
				fileList.add(filePath.toFile().getAbsolutePath());
			});
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot get now local file list because {}", e);
		}
		nowLocalFiles = fileList;		
	}

	public void backupNowLocalFileInfo(String backupFile) {
		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(backupFile));
			for(String file : nowLocalFiles) {
				bufferedWriter.write(file+"\n");
			}
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot back up now local file info because {}", e);
		} finally {
			if(bufferedWriter != null) {
				try {
					bufferedWriter.close();
				} catch (IOException e) {
					LOGGER.error("IoTDB post back sender: cannot close stream after backing up now local file info because {}", e);
				}
			}
		}
	}

	public Set<String> getSendingFiles() {
		return sendingFiles;
	}


	public Set<String> getLastLocalFiles() {
		return lastLocalFiles;
	}


	public Set<String> getNowLocalFiles() {
		return nowLocalFiles;
	}
}
