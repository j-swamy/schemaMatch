from thefuzz import fuzz
from thefuzz import process
from botocore import exceptions 
import json
import os
import boto3
import pandas as pd
import numpy as np
import re

def cleanDataDict(ddObj, filename, sheetname):
    #TODO turn this into a datadict validation tool
    ###If error, return 406 status
    ###DataDict does not meet criteria: "filetype, headers, columns"

    ###If str.endswith('xls*'): require 'sheets_name'
    ###if 'csv', not sheets_name (or ignore)


    """
    Cleans a pandas DataFrame containing a data dictionary by dropping a specific column, grouping by table name, and 
    converting the resulting DataFrame to a dictionary. The resulting dictionary has table names as keys and lists of 
    column names as values.

    Args:
        dataDictDF (pandas.DataFrame): A pandas DataFrame containing a data dictionary.

    Returns:
        dict: A dictionary with table names as keys and lists of column names as values.
    """
    dataDictDF = []
    excelPattern = re.compile(r".+\.xls[a-z]?$")
    if filename.endswith('.csv'):
        dataDictDF = pd.read_csv(ddObj)
    elif excelPattern.search(filename) and sheetname:
        dataDictDF = pd.read_excel(ddObj, sheet_name=sheetname)
    else:
        return {
            'statusCode': 406,
            'body': json.dumps('Data dictionary should be csv or Excel file.')
        }

    ddCols = list(dataDictDF)
    tableNameCol = ""
    colNamesCol = ""
    for col in ddCols:
        if "table" in col.lower():
            tableNameCol = col
        elif "column" in col.lower():
            colNamesCol = col

    if tableNameCol == "" or colNamesCol == "":
        return {
            'statusCode': 406,
            'body': json.dumps('Data Dictionary does not contain the columns table names or column names.')
        }
    
    # dataDictDF = dataDictDF.drop("Change since\nJuly 15, 2020\n(where applicable)", axis=1)
    dataDictDFCols = dataDictDF[[tableNameCol, colNamesCol]]

    ddGrouped = dataDictDFCols.groupby(tableNameCol)[colNamesCol].apply(list).reset_index(name='columns')

    dataDict = ddGrouped.set_index(tableNameCol).T.to_dict('list')

    cleanedData = {}

    for k in dataDict: #dd cleaning
        lst = dataDict[k][0]
        newk = k.strip()
        cleanedData[newk] = [elem.strip() for elem in lst]
    return cleanedData


def sanitize_input(input_str, prefix=None, suffix=None, reserve_char=None):
    """Sanitizes input string based on prefix, suffix, and reserve character"""
    if reserve_char:
        input_str = input_str.replace(reserve_char, '')
    
    if prefix:
        prefix_len = len(prefix)
        if input_str[:prefix_len] == prefix:
            input_str = input_str[prefix_len:]
        
    if suffix:
        suffix_len = len(suffix)
        if input_str[-suffix_len:] == suffix:
            input_str = input_str[:-suffix_len]
    
    return input_str.lower()

def checkNames(dict_str, tbl_str='', tbls=[], prefix=None, suffix=None, reserve_char=None):
    """
    Compares a dictionary string with a table string or a list of table strings and returns a fuzzy score.

    Args:
        dict_str (str): The dictionary string to compare.
        tbl_str (str, optional): The table string to compare. Defaults to ''.
        tbls (list, optional): The list of table strings to compare. Defaults to [].
        prefix (str, optional): The prefix to remove from the strings before comparison. Defaults to None.
        suffix (str, optional): The suffix to remove from the strings before comparison. Defaults to None.
        reserve_char (str, optional): The reserved character to replace with spaces before comparison. Defaults to None.

    Raises:
        Exception: If neither tbl_str nor tbls are provided.

    Returns:
        int or list: If tbl_str is provided, returns the fuzzy score between dict_str and tbl_str.
                    If tbls is provided, returns a list of tuples with the fuzzy scores between dict_str and each table string.
    """
    ds = sanitize_input(dict_str, prefix, suffix, reserve_char)
    
    if tbl_str:
        ts = sanitize_input(tbl_str, prefix, suffix, reserve_char)
        return fuzz.ratio(ds, ts)
    
    elif tbls:
        ts = [sanitize_input(t, prefix, suffix, reserve_char) for t in tbls]
        scores = process.extract(ds, ts, scorer=fuzz.token_sort_ratio)
        return scores
    
    else:
        raise Exception("No table name or list of table names provided")

def findTup(elem, dicts):
    return [item for item in dicts if item[0] == elem]
    
def findMaxDictElems(dicts, tracker, masterDict):
    """
    Given a list of dictionaries, finds the maximum value in each dictionary and the corresponding key.
    If the key has not been seen before, it is added to the tracker dictionary with its corresponding value and dictionary.
    If the key has been seen before, and the new value is greater than the old value, the tracker is updated with the new value and dictionary.
    If the key has been seen before, and the new value is equal to the old value, an exception is raised.
    If the key has been seen before, and the new value is less than the old value, the corresponding value in the master dictionary is removed and the tracker is updated with the new value and dictionary.
    Returns a tuple containing the updated tracker dictionary and a list of tuples to be recalculated.
    """
    recalc = []
    sameScore = []
    for d in dicts:
        dic = d[1]
        dd = d[0]
        for k in dic.keys():
            maxValue = max(dic.values())
            maxMatch = max(dic, key=dic.get)
            if maxMatch not in tracker.keys():
                tracker[maxMatch] = (dd, maxValue)
            elif tracker[maxMatch][1] == maxValue:
                # raise Exception(f"Multiple table names with equal score: {maxMatch} and {tracker[maxMatch][0]}. Please check manually.")
                # sameStringMessage = (f"Multiple table names with equal score: {maxMatch} and {tracker[maxMatch][0]}. Please check manually.", tracker[maxMatch][1])
                sameStringMessage = (maxMatch, tracker[maxMatch][0])
                sameScore.append(sameStringMessage)
            elif tracker[maxMatch][1] < maxValue:
                lst = findTup(tracker[maxMatch][0], masterDict)
                if lst != []:
                    lst[0][1].pop(maxMatch)
                    recalc += lst
                tracker[maxMatch] = (dd, maxValue)
    return (tracker, recalc, sameScore)


def compareColsAlg(dataCols, tblCols, prefix, suffix, reserveChar, threshold=80):
    """
    Compares the columns of a table with the columns of a dataset and returns the matches and unmatched columns.

    Args:
        dataCols (list): A list of columns in the dataset.
        tblCols (list): A list of columns in the table.
        prefix (str): A string to be added as prefix to the column names.
        suffix (str): A string to be added as suffix to the column names.
        reserveChar (str): A character to be used as a reserve character.
        threshold (int, optional): The minimum score required for a match. Defaults to 80.

    Returns:
        tuple: A tuple containing two lists - matches and unmatched columns.
    """
    unmatched =[]
    colLen = len(tblCols)
    dicts = []
    for i in range(colLen):
        col = tblCols[i]["Name"]
        currScoring = checkNames(col, None, dataCols, prefix, suffix, reserveChar)
        dicts += [(col, dict(currScoring))]
    result = findMaxDictElems(dicts, {}, dicts)
    
    sameScore = []
    
    while (result[1] != []):
        # print("enter")
        recalc = result[1]
        tracker = result[0]
        sameScore += result[2]
        result = findMaxDictElems(recalc, tracker, dicts)
        
    finalScores = result[0]
    matches = []
    unmatched = []
    sameScore+=result[2]
    sameScoreSet = list(set(sameScore))
        
    for key in finalScores.keys():
        tup = finalScores[key]
        if (tup[1] > threshold):
            matches += [tup[0]]
        else:
            unmatched += [tup[0]]
            
    unmatchedSameScore = []    
    for t in sameScoreSet:
        if t[1] not in matches:
            unmatchedSameScore += [f"Equal fuzzy score: {t[1]} in data catalog matches {t[0]} in data dictionary. Please check manually."]
            
    return (matches, unmatched, unmatchedSameScore)
    


def lookupTable(tblName, dataDict, prefix, suffix, reserveChar, threshold=100):
    """
    Looks up table in data dictionary and returns table name if found, else returns False

    Args:
        tblName (str): The name of the table to look up
        dataDict (dict): A dictionary containing table names as keys and table data as values
        prefix (str): A string that may appear at the beginning of the table name
        suffix (str): A string that may appear at the end of the table name
        reserveChar (str): A character that may appear in the middle of the table name
        threshold (int, optional): The minimum percent match required for a table name to be considered a match. Defaults to 100.

    Returns:
        str or bool: The name of the matching table if found, else False
    """
    lookupDict = {
        "dict_str":"",
        "tbl_str": tblName,
        "tbls":[],
        "prefix": prefix,
        "suffix": suffix,
        "reserve_char": reserveChar
    }
    for key in dataDict:
        lookupDict["dict_str"] = key
        if checkNames(**lookupDict) == threshold:
            return key
        else:
            continue
    return False
    

def checkUpdatedCols(unmatched, tblCols):
    """
    Removes columns from tblCols that are present in the unmatched list.

    Args:
        unmatched (list): A list of column names that are unmatched.
        tblCols (list): A list of dictionaries representing table columns.

    Returns:
        list: A copy of tblCols with columns present in unmatched removed.
    """
    colLen = len(tblCols)
    tblColsCopy = tblCols.copy()
    for i in range(colLen):
        elem = tblCols[i]
        if tblCols[i]["Name"] in unmatched:
            tblColsCopy.remove(elem)
    return tblColsCopy
       

def lambda_handler(event, context):
    """
    AWS Lambda function that matches the schema of a Glue table with a data dictionary and updates the table's metadata.

    Args:
        event (dict): AWS Lambda uses this parameter to pass in event data to the handler.
        context (object): AWS Lambda uses this parameter to provide runtime information to your handler.

    Returns:
        dict: A dictionary containing the updated metadata of the Glue table.

    Raises:
        Exception: If the data dictionary is not found or the table does not exist in the data dictionary.

    """
    payload = event
    if payload is None:
        print("No payload found")
        return {
            'statusCode': 406,
            'body': json.dumps('No payload found!')
        }
    else:
        print(payload)
    ############Params#######
    prefix = payload.get("prefix",'')
    suffix = payload.get("suffix", '')
    reserveChar = payload.get("reserveChar",'')
    tableName = payload.get("tableName", '')
    ddFile = payload.get("file","")
    sheetName = payload.get("sheetName", "")
    ddBucket = payload.get("bucket","")
    database = payload.get("database","")
    target = payload.get("target", "glue")
    dryrun = payload.get("dryrun",True)
    ########################

    dataDict = ""
    tbl = ""
    match target:
        case "glue": # If using glue, get data dictionary and table using glue methods
            dataDict, tbl = target_glue_get(prefix, suffix, reserveChar, tableName, ddFile, sheetName, ddBucket, database)
    
    print(f"Code is excuting {dryrun=}")
    if dryrun:
        print("Code will only return suggestions as json payload. To have it apply updates, set param 'dryrun: False'")
    else:
        print(f"Code will update values in tbl {tbl=} and return a payload of those it could not match. Set 'dryrun: True' to get a payload of matching values without updating the metadata")

    tblDict = json.loads(tbl)
    tblName = tblDict["Table"]["Name"]
    tblCols = tblDict["Table"]["StorageDescriptor"]["Columns"]
    
    dictTableName = lookupTable(tblName, dataDict, prefix, suffix, reserveChar)
    if not dictTableName:
        return "Table does not exist in data dictionary!"
    dictCols = dataDict[dictTableName][1:]
    results = compareColsAlg(dictCols, tblCols, prefix, suffix, reserveChar)
    matched = results[0]
    unmatched = results[1]
    sameScore = results[2]
    if unmatched == []:
        return "For table " + tblName + ", all columns have been matched."
    
    updatedSchema = checkUpdatedCols(unmatched, tblCols)

    response = ""
    match target:
        case "glue": # Updating table based on glue methods
            response = target_glue_update(tblDict, updatedSchema, dryrun, database)

    for s in sameScore:
        print(s)
    print("For table " + tblName + ", the following columns have not been matched: " + ', '.join(unmatched))
    return response

# Split glue target into two functions-- one for retrieving data and one for updating the table with glue.
def target_glue_get(prefix, suffix, reserveChar, tableName, ddFile, sheetName, ddBucket, database):
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    try:
        ddObj = s3_client.get_object(Bucket=ddBucket,Key=ddFile)
    except exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'NoSuchKey':
            print(f"Data Dictionary Not Found at {ddBucket}/{ddFile}")
            raise 

    dataDict = cleanDataDict(ddObj['Body'], ddFile, sheetName)
    tbl = json.dumps(glue_client.get_table(DatabaseName=database, Name=tableName), default=str)

    return dataDict, tbl

def target_glue_update(targtblDict, updatedSchema, dryrun, database):
    glue_client = boto3.client('glue')
    targtblDict["Table"]["StorageDescriptor"]["Columns"] = updatedSchema
    # newSchema = json.loads(updatedSchema)
    tblDictCopy = dict(targtblDict)
    removeKeys = []
    removeKeys = ['DatabaseName','CreateTime','UpdateTime','CreatedBy','IsRegisteredWithLakeFormation','CatalogId','VersionId','FederatedTable']
    for k in removeKeys:
        if k in tblDictCopy["Table"].keys():
            tblDictCopy["Table"].pop(k)
    if dryrun:
        response = tblDictCopy["Table"]
    else:
        response = glue_client.update_table(DatabaseName=database, TableInput=tblDictCopy["Table"])
    return response

