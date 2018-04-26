package com.biz2tech.indexer.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import com.biz2tech.indexer.SolrIndexer;

public class IndexerUtil {
	private final static String mongoDumpBaseName = "mongo_batch";
	
	public static Properties loadProperties(String propertyfile) throws IOException {
		Properties prop = new Properties();
		prop.load(SolrIndexer.class.getResourceAsStream(propertyfile));
		return prop;
	}
	public static FileWriter createNewBatchFile(String solrBatchOutputDirectory, int batchNum) throws IOException {
		String batchFileName = solrBatchOutputDirectory + "/" + mongoDumpBaseName + "_" + batchNum + ".json";
		File outputJson = new File(batchFileName);
		outputJson.createNewFile();
		FileWriter writer = new FileWriter(outputJson, false);
		return writer;

	}
}
