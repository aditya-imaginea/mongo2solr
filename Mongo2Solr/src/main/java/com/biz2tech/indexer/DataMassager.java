package com.biz2tech.indexer;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Properties;

import com.biz2tech.indexer.util.IndexerUtil;

/*
 * To Split up large data dumps into agreeable batches 
 * pre-processing the large data files
 * 
 */
public class DataMassager {
	public static void main(String args[]) throws IOException {
		Properties props = IndexerUtil.loadProperties("/local/solr.properties");
		FileReader dumpFile = new FileReader(props.getProperty("mongoDumpFile"));
		LineNumberReader lineNumberReader = new LineNumberReader(dumpFile);
		int dumpBatchSize = Integer.parseInt(props.getProperty("batchSizeForMongoDumps"));
		String solrBatchOutputDirectory = props.getProperty("solrBatchBaseDirectory");
        String solrRootElement= props.getProperty("solrRootElement");
		String shipment;
		int batchNumber = 0;
		FileWriter writer = IndexerUtil.createNewBatchFile(solrBatchOutputDirectory, batchNumber);
		writer.write("{ \""+solrRootElement+"\""+": [");
		while ((shipment = lineNumberReader.readLine()) != null) {
			if (lineNumberReader.getLineNumber() % dumpBatchSize == 0) {
				writer.write("	]}");
				writer.flush();
				writer.close();
				batchNumber++;
				writer = IndexerUtil.createNewBatchFile(solrBatchOutputDirectory, batchNumber);
				writer.write("{ \""+solrRootElement+"\""+": [");
			}
			writer.write(shipment + "," + "\n");
		}
		writer.write("	]}");
		writer.flush();
		writer.close();
		lineNumberReader.close();
	}
}
