package com.fdmgroup;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import static tech.tablesaw.aggregate.AggregateFunctions.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;

public class DailyAggregator {

	public static void main(String[] args) {

		//Table object containing raw market data after import from .csv file
		Table rawTable = CsvHandler.createRawTable();
		
		//HashMap holding the weights of each single ticker to calculate the Index
		HashMap<String, Double> indexWeightsMap = CsvHandler.createIndexWeightsHashMap();
	
		//Retrieve price column from raw Data Table
		IntColumn priceColumn = (IntColumn) rawTable.column(2);
		
		//Calculate prices column correctly with decimal points
		DoubleColumn priceColumnFloat = priceColumn.divide(100);
		
		//Replace price IntColumn with correct price DoubleColumn due to "," being the decimal sign
		rawTable.replaceColumn(2, priceColumnFloat);

		//Set correct column names as defined in the description
		rawTable.column(0).setName("date+time");
		rawTable.column(1).setName("ticker");
		rawTable.column(2).setName("price");
		rawTable.column(3).setName("number of securities traded");

		//Create list with every included ticker in the data set
		@SuppressWarnings("unchecked")
		Set<String> tickerSet = (Set<String>) rawTable.column("ticker").asSet();
		List<String> tickerList = new ArrayList<>(tickerSet);

		//Create explicit column objects from the raw table for later calculations
		DateTimeColumn dateAndTime = (DateTimeColumn) rawTable.column("date+time");
		StringColumn ticker = (StringColumn) rawTable.column("ticker");
		IntColumn numberOfSecuritiesTraded = (IntColumn) rawTable.column("number of securities traded");
		DoubleColumn dailyTradedVolume = (DoubleColumn) priceColumnFloat.multiply(numberOfSecuritiesTraded);

		//Create list with every included date in the data set
		Set<LocalDate> dateSet = dateAndTime.date().asSet();
		List<LocalDate> dateList = new ArrayList<>(dateSet);

		//Insert index prices into raw data table
		insertIndexIntoRawData(rawTable, indexWeightsMap, tickerList);

		//Sort table according to date+time column, since inserted rows are inserted at the bottom of the table
		rawTable.sortAscendingOn("date+time");
		
		//Update respective variables after Index was added to the data
		dateAndTime = (DateTimeColumn) rawTable.column("date+time");
		ticker = (StringColumn) rawTable.column("ticker");
		numberOfSecuritiesTraded = (IntColumn) rawTable.column("number of securities traded");
		dailyTradedVolume = (DoubleColumn) priceColumnFloat.multiply(numberOfSecuritiesTraded).roundInt();

		//Aggregate daily prices, volume and index values by the given parameters: open, close, highest, lowest, volume
		Table aggregateTable = rawTable.summarize(priceColumnFloat, first, last, max, min).by(dateAndTime.date(), ticker);
		Table dailyVolume = rawTable.summarize(dailyTradedVolume, sum).by(dateAndTime.date(), ticker);

		//Extract volume column from dailyVolume Table
		Column<?> dailyVolumeColumn = dailyVolume.column(2);

		//Add dailyVolume as column to aggregateDailyTable
		Table aggregateDailyTableWithMissingValues = aggregateTable.addColumns(dailyVolumeColumn);

		//Set the correct column names of the daily aggregate Table 
		aggregateDailyTableWithMissingValues.column(0).setName("date");
		aggregateDailyTableWithMissingValues.column(1).setName("ticker");
		aggregateDailyTableWithMissingValues.column(2).setName("open");
		aggregateDailyTableWithMissingValues.column(3).setName("close");
		aggregateDailyTableWithMissingValues.column(4).setName("highest");
		aggregateDailyTableWithMissingValues.column(5).setName("lowest");
		aggregateDailyTableWithMissingValues.column(6).setName("volume");

		//Check for tickers which have not been traded in a single day and add those to the daily aggregates with NaN
		insertMissingRows(aggregateDailyTableWithMissingValues, tickerList, dateList);
		
		//Sort table according to date, ticker
		Table finalOutputDailyAggregates = aggregateDailyTableWithMissingValues.sortAscendingOn("date", "ticker");
		
		//Print out short summary about missing values
		System.out.println(finalOutputDailyAggregates.missingValueCounts().print());//.summarize("open", max));
		
		//Print out a short summary of the final output table
		System.out.println(finalOutputDailyAggregates.print());
		
		//Option to print every single line on its own
		printfullTable(finalOutputDailyAggregates);
		
		CsvHandler.exportTableToCSV(finalOutputDailyAggregates);

	}

	/**
	 * Prints every row of a table in a separate output
	 * @param dataTable The table to print out
	 */
	private static void printfullTable(Table dataTable) {
		for (Row rowToIterate : dataTable) {
			System.out.println(rowToIterate);
		}
		
	}

	/**
	 * Inserts the missing rows into a given Table. If there are trading days during which any ticker is not traded, those rows
	 * are filled with Double.NaN to complement the final output despite not having any recorded trades for those days.
	 * @param dataTable The table in which the rows for missing values are added.
	 * @param tickerList The list of all tickers available.
	 * @param dateList The list of all dates available.
	 */
	private static void insertMissingRows(Table dataTable, List<String> tickerList, List<LocalDate> dateList) {
		
		//Iterate over all available dates in the Table
		for (int i = 0; i < dateList.size(); i++) {

			Table testNA = dataTable.where(dataTable.dateColumn("date").isEqualTo(dateList.get(i)));
			for (int j = 0; j < tickerList.size(); j++) {
				//Check whether every ticker is represented in every date, does not check in case no ticker is traded in a day -> 
				//problem when there are outliers which are not holidays/weekends
				if (!testNA.column("ticker").asList().contains(tickerList.get(j))) {
					
					//Create row to add to the table
					Table insertMissingRowWithNA = Table.create();

					DateColumn dateColumnForNA = DateColumn.create("date", dateList.get(i));
					insertMissingRowWithNA.addColumns(dateColumnForNA);

					StringColumn tickerColumnForNA = StringColumn.create("ticker", tickerList.get(j));
					insertMissingRowWithNA.addColumns(tickerColumnForNA);

					DoubleColumn openColumnForNA = DoubleColumn.create("open", Double.NaN);
					insertMissingRowWithNA.addColumns(openColumnForNA);

					DoubleColumn closeColumnForNA = DoubleColumn.create("close", Double.NaN);
					insertMissingRowWithNA.addColumns(closeColumnForNA);

					DoubleColumn highestColumnForNA = DoubleColumn.create("highest", Double.NaN);
					insertMissingRowWithNA.addColumns(highestColumnForNA);

					DoubleColumn lowestColumnForNA = DoubleColumn.create("lowest", Double.NaN);
					insertMissingRowWithNA.addColumns(lowestColumnForNA);

					DoubleColumn volumeColumnForNA = DoubleColumn.create("volume", Double.NaN);
					insertMissingRowWithNA.addColumns(volumeColumnForNA);

					dataTable.addRow(0, insertMissingRowWithNA);
				}
			}
		}
	}

	/**
	 * Calculates index prices based on set weights and inputs respective rows into raw data.
	 * @param rawTable The raw data table to insert the index rows into.
	 * @param indexWeightsMap Weights of respective ticker prices to calculate the index from.
	 * @param tickerList The list of all tickers available to calculate the index from.
	 */
	private static void insertIndexIntoRawData(Table rawTable, HashMap<String, Double> indexWeightsMap,
			List<String> tickerList) {
		//Create HashMap to calculate index value and insert into data
		/* Since Keys are unique, values are overridden when new values are iterated over, so state of HashMap is
			always current price of index */
		HashMap<String, Double> indexTickerMap = new HashMap<>();
		
		for (int i = 0; i < rawTable.rowCount(); i++) {
			
			//Insert the every value of each single ticker into the HashMap
			indexTickerMap.put(rawTable.column("ticker").getString(i), (Double) rawTable.column("price").get(i));
			
			//If the HashMap is not yet fully filled with all ticker prices to calculate index, continue.
			if (!(indexTickerMap.size() == tickerList.size())) {
				continue;
			}
				
				//Create the Row to be added to the table manually
				Table insertIndexTable = Table.create();

				DateTimeColumn dateTimeColumnIndex = DateTimeColumn.create(rawTable.column("date+time").name(),
						(LocalDateTime) rawTable.column("date+time").get(i));
				insertIndexTable.addColumns(dateTimeColumnIndex);

				StringColumn tickerColumnIndex = StringColumn.create(rawTable.column("ticker").name(), "INDEX");
				insertIndexTable.addColumns(tickerColumnIndex);

				Double indexValue = 0.0;
				for (int j = 0; j < indexWeightsMap.size(); j++) {
					indexValue += indexWeightsMap.get(tickerList.get(j)) * indexTickerMap.get(tickerList.get(j));
				}
				
				indexValue = (Math.round(indexValue * 100) / 100.0);

				DoubleColumn priceColumnIndex = DoubleColumn.create(rawTable.column("price").name(), indexValue);
				insertIndexTable.addColumns(priceColumnIndex);

				DoubleColumn volumeColumnIndex = DoubleColumn
						.create(rawTable.column("number of securities traded").name(), 0);
				insertIndexTable.addColumns(volumeColumnIndex);

				rawTable.addRow(0, insertIndexTable);
		}
	}

}
