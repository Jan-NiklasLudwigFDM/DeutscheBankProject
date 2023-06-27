package com.fdmgroup;

import java.util.HashMap;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

public class CsvHandler {
	
	//Values are set according to the configuration of the file path
	private static final String rawMarketDataPath = "/Users/Jan/Documents/Documents/FDM Group Trainee/DeutscheBank/test-market.csv";
	private static final String indexWeightsPath = "/Users/Jan/Documents/Documents/FDM Group Trainee/DeutscheBank/index-weights.csv";
	private static final String exportCSVPath = "/Users/Jan/Documents/Documents/FDM Group Trainee/DeutscheBank/dailyAggregates.csv";
	private static final char separator = ';';
	
	/**
	 * Exports the given Table to a .csv file
	 * @param exportTable The table to export
	 */
	public static void exportTableToCSV(Table exportTable) {
		exportTable.write().csv(exportCSVPath);
	}
	
	/**
	 * Creates the table object of data from a file path to a .csv file and the respective delimiter 
	 * @param filePath The path of the raw data .csv file.
	 * @param separator The country-specific separator used in the .csv file.
	 * @return A table with columns and rows representing the raw data as Table object.
	 */
	public static Table createRawTable() {

		// Create read-in options with ; as seperator
		CsvReadOptions.Builder builder = CsvReadOptions.builder(rawMarketDataPath).separator(separator).header(false);
		CsvReadOptions options = builder.build();

		// Finished table with all needed attributes defined in CsvReadOptions
		Table finishedTable = Table.read().usingOptions(options);

		return finishedTable;
	}
	
	
	/**
	 * Creates a HashMap according to the given .csv file with weights for calculating the Index
	 * @param filePath Path to the .csv file with weights of index calculation
	 * @param separator The delimiter used to separate data
	 * @return
	 */
	public static HashMap<String, Double> createIndexWeightsHashMap() {

		// Create read-in options with ; as seperator
		CsvReadOptions.Builder builder = CsvReadOptions.builder(indexWeightsPath).separator(separator).header(false);
		CsvReadOptions options = builder.build();

		// Finished table with all needed attributes defined in CsvReadOptions
		Table indexCalculationTable = Table.read().usingOptions(options);
		
		HashMap<String, Double> indexHashMap = new HashMap<>();
		for (Row c : indexCalculationTable) {
			indexHashMap.put(c.getString(0), c.getInt(1) / 10.0);
		}
		return indexHashMap;
	}

	//Getters & Setters
	public static String getRawMarketDataPath() {
		return rawMarketDataPath;
	}


	public static String getIndexWeightsPath() {
		return indexWeightsPath;
	}

	public static char getSeparator() {
		return separator;
	}

}
