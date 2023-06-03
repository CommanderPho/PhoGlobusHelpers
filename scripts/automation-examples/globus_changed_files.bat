@echo off

REM Set your Globus access token and endpoint information
SET ACCESS_TOKEN=769d24e1-d1cc-4198-9ff7-2626485da449
SET SOURCE_ENDPOINT_ID= 8c185a84-5c61-4bbc-b12b-11430e20010f
SET START_TIME=2023-05-31T00:00:00Z

REM List the files on the endpoint and filter based on modified timestamp
globus ls %SOURCE_ENDPOINT_ID%:/ --format=unix --access-token=%ACCESS_TOKEN% --recursive --time-modified=%START_TIME% | findstr /R /C:"^f"
