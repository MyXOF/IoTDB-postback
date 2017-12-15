package cn.edu.tsinghua.postback.sender;

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
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.postback.Config;

public class FileManager {
	private Set<String> fileList = new HashSet<>();
	private static final Logger LOGGER = LoggerFactory.getLogger(FileManager.class);
	private static class FileManagerHolder{
		private static final FileManager INSTANCE = new FileManager();
	}
	
	private FileManager() {}

	public static final FileManager getInstance() {
		return FileManagerHolder.INSTANCE;
	}

	public Set<String> getSendingFileList(Set<String> oldFiles, Set<String> newFiles) {
		Set<String> sendingFiles = new HashSet<>();
		for(String newFile : newFiles) {
			if(!oldFiles.contains(newFile)) {
				sendingFiles.add(newFile);
			}
		}
		return sendingFiles;
	}

	public Set<String> getLastLocalFileList(String path)  {
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
		return fileList;
	}

	public Set<String> getNowLocalFileList(String path) {
		/*
		Set<String> fileList = new HashSet<>();
		try (Stream<Path> filePathStream = Files.walk(Paths.get(path))) {
			filePathStream.filter(Files::isRegularFile);
			// TODO filter correct file
			filePathStream.forEach(filePath -> {
				fileList.add(filePath.toFile().getAbsolutePath());
			});
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot get now local file list because {}", e);
		}
		return fileList;
		*/
		fileList.clear();
		getFileList(path);
			return fileList;
				
	}

	//Recursively loop through the list of all files
	public void getFileList(String filePath) {
		File root = new File(filePath);
		File[] files = root.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				getFileList(file.getPath()); 
			} else {
				String filename = file.getPath();
				fileList.add(filename);
			}
		}
	}


	public void backupNowLocalFileInfo(String dataDirectory, String backupFile) {
		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(backupFile));
			for(String file : getNowLocalFileList(dataDirectory)) {
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
}
