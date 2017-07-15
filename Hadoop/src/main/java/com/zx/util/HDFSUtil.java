package com.zx.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public class HDFSUtil {

	private static final Logger LOG = Logger.getLogger(HDFSUtil.class);
	private static FileSystem filesystem;
	static {

		Configuration conf = new Configuration();
		URI uri = URI.create("hdfs://master:9000/");
		try {
			filesystem = FileSystem.get(uri, conf);
			LOG.debug("文件系统创建成功");
		} catch (IOException e) {
			LOG.error("文件系统创建失败", e);
			System.exit(-1);
		}

	}

	/**
	 * 查看文件内容
	 * 
	 * @param infile
	 *            文件在hdfs上的位置
	 * @param outfile
	 *            导出内容位置,不指定位置，默认在控制台输出
	 */
	public static void readFile(String infile, OutputStream out) {

		Path inpath = new Path(infile);
		FSDataInputStream in = null;
		try {

			in = filesystem.open(inpath, 4096);
			LOG.debug("打开文件系统成功");

		} catch (IOException e) {

			LOG.error("打开文件系统失败", e);
			throw new RuntimeException(e);
		}

		if (out == null) {
			out = System.out;
		}

		try {
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (IOException e) {
			LOG.error(e);
		}

	}

	/**
	 * 下载资源
	 * 
	 * @param srcpath
	 *            FS上路径
	 * @param path
	 *            本地路径
	 * @return 下载成功返回true，else false
	 */
	public static boolean download(String srcpath, String path) {

		try {
			filesystem.copyToLocalFile(new Path(srcpath), new Path(path));
			return true;
		} catch (IllegalArgumentException e) {
			LOG.error("地址不正确", e);
		} catch (IOException e) {
			LOG.error("下载失败", e);
		}

		return false;

	}

	/**
	 * 删除文件
	 * 
	 * @param filepath
	 *            文件路径
	 * @param recursive
	 *            是否递归
	 * @return 删除文件成功返回true，else false
	 */
	public static boolean del(String filepath, boolean recursive) {

		Path path = new Path(filepath);
		try {
			filesystem.delete(path, recursive);
			return true;
		} catch (IOException e) {
			LOG.error("删除失败...", e);
		}

		return false;
	}

	/**
	 * 追加文件，当文件不存在时，创建
	 * 
	 * @param path
	 *            路径
	 * @param content
	 *            追加的内容
	 * @param isAppend
	 *            是否追加，还是覆盖
	 * @return 追加成功返回true ，else false
	 */
	public static boolean appendFile(String path, String content, boolean isAppend) {

		Path file = new Path(path);

		FSDataOutputStream out = null;

		try {
			if (filesystem.exists(file) && isAppend) {
				out = filesystem.append(file, 4096);
				LOG.info("文件存在，开始追加");
			} else {
				LOG.info("文件不存在，创建文件");
				out = filesystem.append(file, 4096);
			}

			ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());
			IOUtils.copyBytes(in, out, 4096, true);

			return true;
		} catch (IOException e) {
			LOG.error("追加错误...", e);
			return false;
		}

	}

	/**
	 * 得到文件的元信息
	 * 
	 * @param infile
	 *            文件路径
	 * @return 元信息
	 */
	public static FileStatus getFileStatus(String infile) {
		try {
			return filesystem.getFileStatus(new Path(infile));
		} catch (IllegalArgumentException | IOException e) {
			LOG.error("获取文件元信息失败", e);
			return null;
		}
	}

	/**
	 * 创建目录
	 * 
	 * @param dirPath
	 *            目录位置
	 * @return 成功返回ture，失败 false
	 */
	public static boolean creatDir(String dirPath) {

		Path path = new Path(dirPath);
		try {
			if (filesystem.exists(path)) {
				LOG.info("目录存在,无需创建");
				return false;
			}

			return filesystem.mkdirs(path);

		} catch (IOException e) {
			LOG.error("创建目录失败", e);
			return false;
		}

	}

}
