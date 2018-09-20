Notes on using the PullPointsFromE3OS.py python script

0 - pip install these python packages

- import datetime
- import pymssql
- import requests
- import json
- import urllib
- import os
- import xlrd

1 - Create a directory PythonScriptConfigDir on you desktop

2 - Create and evnironment variable that exposes the path to this folder

e.g., mine is

export EDGE_PYTHON_SCRIPTS_CONFIG_HOME=/Users/hal/Desktop/PythonScriptConfigDir

in windows you would create and enviroment variable in the GUI, something like:

key - EDGE_PYTHON_SCRIPTS_CONFIG_HOME

value - c:\Users\ClarksDesktop\Tools\PythonScriptConfigDir

3 - in this folder you would store

- config.text - keys and values for various urls, users, passwords etc
- livepoints.xlsx - the points that you want to query from e3os, and push the corresponding values to edge


4 - open the script with a text edtior and set these values appropriately (starting on about 415)
    if siteToLoad == 'YouFavoriteSiteName':
        fromDateString = '2017-01-01T00:00:00.000Z'
        toDateString = '2017-07-26T23:59:55.000Z'
        envType = 'OEDEV'
        stationSid = 'c:whatcomindustries.s:demo-edge.st:1'
        stationID = 'demo_f0258b66'
        stationName = 'demo_f0258b66'
        sendingStationName = 'demo_f0258b66'
        pointsListFileName = 'livepointsDemoDev.xlsx'
        qualifier = 'THPH.THC.THCEDGE.THCEDGE.'

 5 - run the script from the command line by typing "python PullPointsFromE3OS.py YouFavoriteSiteName"



