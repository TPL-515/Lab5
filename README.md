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

1) Look in the U.I. and find the "generate data" job

2) Materialize the job and look at the data preview.

3) Using existing assets write a job that adds a row of randomly generated data to the database.

4) Have the job run on a cron every 1 min.

5) Use the "view database" asset to view what is currently in the database and validate that it is updating.

