package cn.edu.tsinghua.postback;

import java.io.File;

public class Config {
	public static final String POST_BACK_DIRECTORY = "postback"+ File.separator;
	public static final String UUID_PATH = POST_BACK_DIRECTORY + "uuid.txt";
	public static final String LAST_FILE_INFO = POST_BACK_DIRECTORY + "lastLocalFileList.txt";
	public static final String SENDER_FILE_PATH = POST_BACK_DIRECTORY + "data";
	public static final String SNAPSHOT_PATH = POST_BACK_DIRECTORY + "dataSnapshot";
}
