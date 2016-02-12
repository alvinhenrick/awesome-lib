package com.example.awesome;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.kitesdk.data.hbase.avro.AvroUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Alvin on 2/11/16.
 */
public class HandleSchemaVersion
{

	public static void main(String[] args) throws Exception
	{
		try
		{
			final String stagingPrefix = args[0].trim();
			final String metadataStorePrefix = args[1].trim();
			final String dataBaseName = args[2].trim().toLowerCase();
			final String tableName = args[3].trim().toLowerCase();

			final String avroFileDir = stagingPrefix + "/" + dataBaseName + "/" + tableName;

			final Path existingSchemaPath = new Path(
					metadataStorePrefix + "/" + dataBaseName + "/" + tableName + "/current/" + tableName + ".avsc");

			final Path backupSchemaPath = new Path(metadataStorePrefix + "/" + dataBaseName + "/" + tableName + "/"
					+ getFolderName() + "/" + tableName + ".avsc");

			final Configuration conf = new Configuration();
			// conf.set("fs.defaultFS", "hdfs://ip:8020");

			final FileSystem fs = FileSystem.get(conf);

			final Schema newSchema = getSchema(fs, avroFileDir);

			if ( fs.exists(existingSchemaPath) )
			{
				final Schema existingSchema = readExistingSchema(fs, existingSchemaPath);

				if ( AvroUtils.avroSchemaTypesEqual(newSchema, existingSchema) )
				{
					// Don't Do anything
				}
				else
				{
					// Take Backup
					copyFile(fs, backupSchemaPath, existingSchema);

					// Overwrite with new schema
					copyFile(fs, existingSchemaPath, newSchema);
				}
			}
			else
			{
				// Create new schema
				copyFile(fs, existingSchemaPath, newSchema);
			}

		}
		catch ( IOException e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	private static Schema getSchema(final FileSystem fs, final String avroFileDir) throws Exception
	{
		final Path inPath = firstFile(fs, avroFileDir);
		final BufferedInputStream dataInputStream = new BufferedInputStream(fs.open(inPath));

		final DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(dataInputStream, reader);

		return dataFileReader.getSchema();
	}


	public static Path firstFile(final FileSystem fs, final String dir) throws Exception
	{
		final PathFilter avroFilter = new PathFilter() {
			public boolean accept(Path file)
			{
				return file.getName().endsWith(".avro");
			}
		};
		final FileStatus[] status = fs.listStatus(new Path(dir), avroFilter);

		if ( null != status && status.length > 0 )
		{
			return status[0].getPath();
		}
		else
		{
			return null;
		}
	}


	public static Schema readExistingSchema(final FileSystem fs, final Path inPath) throws Exception
	{
		final BufferedInputStream existingInputStream = new BufferedInputStream(fs.open(inPath));
		return new Schema.Parser().parse(existingInputStream);
	}


	public static void copyFile(final FileSystem fs, final Path destinationPath, final Schema schema) throws Exception
	{
		final OutputStream outputStream = fs.create(destinationPath, true);
		final InputStream inputStream = new ByteArrayInputStream(schema.toString(true).getBytes());
		IOUtils.copyBytes(inputStream, outputStream, 4096, true);
	}


	public static String getFolderName()
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd_HHmm");
		return "v_" + format.format(new Date());
	}

}
