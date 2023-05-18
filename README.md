# Lab 5

For this lab we will be creating scheduled runs for our jobs and manipulating their run intervals.

## Getting Started

First clone the lab locally and install the dependencies like so:

```bash
git clone git@github.com:TPL-515/Lab5.git
cd Lab5/
pip install -e ".[dev]"
```

This should install all of the required dependencies for the lab.

## Running the Lab

In order to run the lab just run the following command:

```bash
dagster dev
```

From here you should be able to navigate to the UI hosted at: http://localhost:3000

## Lab Tasks

For this lab you will be asked to perform the following tasks

1) Look in the U.I. and find "crud" code. Make sure to read through and understand what it is doing.

2) Write an asset called "create_demo_table" that creates a table from within our database and performs error logging.

3) Write an asset called "display_table_before" that logs the number of rows and columns for the data in the database before ingesting the data

4) Write an asset called "ingest_data" that is configurable with dagsters built in configuration and ingests nrows of data based on the config file.

5) Write an asset called "display_table_after" that logs the number of rows and columns for the data in the database after ingesting the data

6) Write an asset called "clear_table" that clears all the data from the database

7) Write a job called "generate_data" that runs the assets 2-5 in order.

8) Write a job called " remove_data" that runs asset 6

9) Run these from the UI and confirm that they work properly

10) Change your config and run through validating this works.

11) Optionally create a schedule for the generate data job to run.

