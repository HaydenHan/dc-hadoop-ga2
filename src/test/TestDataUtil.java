package test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestDataUtil {

	protected static List<String> readFromLocalLog(String logFilePath) throws Exception {
		List<String> dataList = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(logFilePath)));
		String line = null;

		while (null != (line = br.readLine())) {
			dataList.add(line);
		}
		br.close();
		return dataList;
	}

	protected static List<String> readFromLogHDFS(String logFilePath) {
		List<String> dataList = new ArrayList<String>();
		Configuration conf = new Configuration();
		InputStream in = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(logFilePath), conf);
			in = fs.open(new Path(logFilePath));
			BufferedReader read = new BufferedReader(new InputStreamReader(in));
			String line = null;
			try {
				while ((line = read.readLine()) != null) {
					dataList.add(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return dataList;
	}

}
