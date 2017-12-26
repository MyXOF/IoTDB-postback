package cn.edu.tsinghua.iotdb.postback;

import java.io.File;

public class Config {
	public static final String POST_BACK_DIRECTORY = "D:" + File.separator +"iotdb-v0.3.0" + File.separator +"data" + File.separator;
	public static final String UUID_PATH = POST_BACK_DIRECTORY + "uuid.txt";
	public static final String LAST_FILE_INFO = POST_BACK_DIRECTORY + "lastLocalFileList.txt";
	public static final String SENDER_FILE_PATH = POST_BACK_DIRECTORY + "delta";
	public static final String SNAPSHOT_PATH = POST_BACK_DIRECTORY + "dataSnapshot";
	public static final String SCHEMA_PATH = POST_BACK_DIRECTORY + "metadata" + File.separator + "mlog.txt";
	public static final String SERVER_IP = "127.0.0.1";
	public static final int SERVER_PORT = 5555;
}
