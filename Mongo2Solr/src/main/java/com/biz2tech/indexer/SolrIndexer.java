package com.biz2tech.indexer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;

public class SolrIndexer {
	private final static String solrInputBaseName = "solr_input";

	public static void main(String[] args) throws IOException, ParseException, SolrServerException {

		// Get the necessary properties for current environment
		Properties solrProperties = loadProperties();
		String solrUrl = solrProperties.getProperty("updateShipmentUrl");
		String solrBatchBaseDirectory = solrProperties.getProperty("solrBatchBaseDirectory");
		Integer solrBatchSize = Integer.parseInt(solrProperties.getProperty("batchSizeForSolrCommit"));
		String solrInputPrefix = solrProperties.getProperty("solr_input_prefix");
		String mongoInputPrefix = solrProperties.getProperty("mongo_input_prefx");
		String transformFile = solrProperties.getProperty("transformFileJson");

		List chainrSpecJSON = JsonUtils.classpathToList(transformFile);
		Chainr chainr = Chainr.fromSpec(chainrSpecJSON);

		// Processing
		createSolrInputBatchFiles(chainr, solrBatchSize, solrBatchBaseDirectory, mongoInputPrefix);
		indexSolrBatches(solrUrl, solrBatchBaseDirectory, solrInputPrefix);
	}

	private static Properties loadProperties() throws IOException {
		Properties prop = new Properties();
		prop.load(SolrIndexer.class.getResourceAsStream("/local/solr.properties"));
		return prop;
	}

	private static void createSolrInputBatchFiles(Chainr chainr, int batchSize, String solrBatchOutputDirectory,
			String mongoInputPrefix) throws IOException, ParseException {

		File[] mongoBatches = getBatchFiles(new File(solrBatchOutputDirectory), mongoInputPrefix);
		System.out.println("Found " + mongoBatches.length + " mongo batches for solr format conversion");
		// For Each mongo Batch create the solr input batches
		for (File mongoBatch : mongoBatches) {
			System.out.println(mongoBatch.getName());
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject) parser.parse(new InputStreamReader(new FileInputStream(mongoBatch)));
			JSONArray shipments = (JSONArray) jsonObject.get("shipments");

			int count = 1;
			int batchNum = 1;
			FileWriter writer = createNewBatchFile(solrBatchOutputDirectory, batchNum, mongoBatch.getName());
			writer.write("[");

			for (Object shipment : shipments) {
				if ((count % batchSize == 1) && (batchNum != 1)) {
					writer = createNewBatchFile(solrBatchOutputDirectory, batchNum, mongoBatch.getName());
					writer.write("[");
					System.out.println("Creating batch for :" + mongoBatch.getName() + " " + batchNum);
				}
				String shipmentTranformed = JsonUtils.toJsonString(chainr.transform(shipment));
				writer.write(shipmentTranformed);
				if (count == batchSize || (count + (batchNum - 1) * batchSize == shipments.size())) {
					writer.write("]");
					writer.flush();
					writer.close();
					batchNum++;
					count = 0;
				} else {
					writer.write(",");
				}
				count++;
			}
		}
		for (File mongoFile : mongoBatches)
			mongoFile.renameTo(new File(mongoFile.getCanonicalPath() + ".processed"));

	}

	private static FileWriter createNewBatchFile(String solrBatchOutputDirectory, int batchNum, String inputFileName)
			throws IOException {
		String batchFileName = solrBatchOutputDirectory + "/" + solrInputBaseName + "_" + batchNum + "."
				+ inputFileName;
		File outputJson = new File(batchFileName);
		outputJson.createNewFile();
		FileWriter writer = new FileWriter(outputJson, false);
		return writer;

	}

	private static File[] getBatchFiles(File InputDir, String pattern) {
		return InputDir.listFiles(new SolrFileNameFilter(pattern));
	}

	private static class SolrFileNameFilter implements FilenameFilter {
		private String prefix;

		public SolrFileNameFilter(String prefix) {
			this.prefix = prefix.toLowerCase();
		}

		public boolean accept(File dir, String fileName) {
			return (fileName.toLowerCase().startsWith(prefix) && !fileName.toLowerCase().endsWith(".processed"));
		}
	}

	private static void indexSolrBatches(String solrUrl, String solrInputDirectory, String solrInputPrefix)
			throws IOException, SolrServerException {
		File[] files = getBatchFiles(new File(solrInputDirectory), solrInputPrefix);
		long startTime = System.currentTimeMillis();
		HttpSolrClient solrClient = new HttpSolrClient(solrUrl);
		ContentStreamUpdateRequest jsonRequest = new ContentStreamUpdateRequest("/update");
		for (File solrInputBatch : files) {
			System.out.println("Indexing started for batch " + solrInputBatch.getName());
			jsonRequest.addFile(solrInputBatch, "application/json");
			jsonRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
			UpdateResponse response = jsonRequest.process(solrClient);
			System.out.println(response.toString());
		}

		long endTime = System.currentTimeMillis();
		System.out.println("That took " + (endTime - startTime) + " milliseconds");

		for (File solrInputBatch : files)
			solrInputBatch.renameTo(new File(solrInputBatch.getCanonicalPath() + ".processed"));

	}

}
