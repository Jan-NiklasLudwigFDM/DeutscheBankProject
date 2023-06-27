# DeutscheBankProject - Jan-Niklas Ludwig

This project is subject to an interview at Deutsche Bank for the position of a Java Software Developer.
As part of the application process, applicants were tasked to perform computations on artifical financial market data incl. daily aggregations and index calculations.
The implementation is done via a Maven Project which can be run in every common Java IDE.

# Code source and set up

Before running the program, there are the following steps to follow:
1. Clone the repository (https://github.com/Jan-NiklasLudwigFDM/DeutscheBankProject.git) and open the poject.
2. The project consists of two classes: Daily Aggregator and Csv Handler.
3. Before running Daily Aggregator which performs all calculation related tasks, set up the needed paths to the .csv files in the CsvHandler class.
4. 3 file paths are required to be set up: The log data of the market assets, a .csv file containing the weights of the index and the preferred path of the output file.
5. Run Daily Aggregator, output is generated once in the standard output, but also in a .csv file called dailyAggregates.

# Code walkthrough

The java library tablesaw is used to perform various data engineering tasks throughout the application.
To give a brief overview over the project:
1. The market log data is read into the application.
2. Data is initially formatted to fit the underlying structure of the .csv source.
3. The index is calculated and added to the data table.
4. Daily aggregations are performed on each ticker as well as the index if applicable (open, close, highest, lowest, volume).
5. Missing values are added in case tickers were not traded on particular days.
6. Output is generated in both the console as well as an external .csv file.
