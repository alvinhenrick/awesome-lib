package com.example.awesome;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Vector;

/**
 * Created by Alvin on 2/12/16.
 */
public class ScpToHDFS
{

	public static void main(String[] args) throws Exception
	{
		final String hostname = args[0].trim();
		final String username = args[1].trim();
		final String passwwordFile = args[2].trim();
		final String sourceDirectory = args[3].trim();
		final String destinationDirectory = args[3].trim();

		final Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(conf);

		final String strPassword = getPassWord(fs, passwwordFile);

		uploadToHDFS(fs, hostname, username, strPassword, sourceDirectory, destinationDirectory);
	}


	@SuppressWarnings("unchecked")
	public static void uploadToHDFS(final FileSystem fs, final String hostname, final String username,
			final String password, final String directoryWithPattern, final String destinationDirectory) throws Exception
	{

		java.util.Properties config = new java.util.Properties();
		config.put("StrictHostKeyChecking", "no");

		JSch ssh = new JSch();
		Session session = ssh.getSession(username, hostname, 22);
		session.setConfig(config);
		session.setPassword(password);
		session.connect();
		Channel channel = session.openChannel("sftp");
		channel.connect();

		ChannelSftp sftp = (ChannelSftp)channel;
		Vector<ChannelSftp.LsEntry> files = sftp.ls(directoryWithPattern);

		System.out.printf("Found %d files in dir %s%n", files.size(), directoryWithPattern);

		for ( ChannelSftp.LsEntry file : files )
		{
			if ( !file.getAttrs().isDir() )
			{
				System.out.printf("Reading file : %s%n", file.getFilename());

				final InputStream inputStream = sftp.get(file.getFilename());

				Path destinationPath = new Path(destinationDirectory + "/" + file.getFilename());
				final OutputStream outputStream = fs.create(destinationPath, true);

				IOUtils.copyBytes(inputStream, outputStream, 4096, true);
			}
		}
		channel.disconnect();
		session.disconnect();
	}


	public static String getPassWord(final FileSystem fs, final String passwwordFile) throws Exception
	{
		final Path path = new Path(passwwordFile);
		final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line = bufferedReader.readLine();
		while ( line != null )
		{
			line = bufferedReader.readLine();
		}
		return line.trim();
	}
}
