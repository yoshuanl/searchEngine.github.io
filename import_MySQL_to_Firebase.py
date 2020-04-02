#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Execution format: python import.py <mysql-database-name> <firebase-database-node-name>
"""

"""
Employees:
    tables: employees, departments, titles, dept_emp
    we only take employees with ID <= 11000
Sakila (film):
    tables: actor, film_actor, film
    select columns in film: film_id, title, description, length
"""

import sys
import mysql.connector
import string
import decimal
import datetime
import json
import collections
import requests

picked_cols = {"film": ["film_id", "title", "description", "length"],
               "actor": ["actor_id", "first_name", "last_name"],
               "film_actor": ["film_id", "actor_id"]}

primary_key = {"world":
                {"country": "CountryCode",
                 "countrylanguage": "CountryLanguage",
                 "city": "ID"},
                 
            "employees":
                {"employees": "emp_no",
                 "titles": "EmpDate",
                 "departments": "dept_no",
                 "dept_emp": "EmpDept"},
                 
            "sakila":
                {"film": "film_id",
                 "actor": "actor_id",
                 "film_actor": "FilmActor"}}
                    
link_cols = {"world":
            {"country": ["CountryCode"],
             "countrylanguage": ["CountryCode"],
             "city": ["CountryCode"]},
             
            "employees":
            {"employees": ["emp_no"],
             "titles": ["emp_no"],
             "departments": ["dept_no"],
             "dept_emp": ["emp_no","dept_no"]},
             
            "sakila":
            {"film": ["film_id"],
             "actor": ["actor_id"],
             "film_actor": ["film_id", "actor_id"]}}

# columns that represent ID and don't need number formatting
avoid_nb_format = {"world": set(["ID", "Capital", "IndepYear"]),
                   "employees": set(["emp_no", "EmpDept"]),
                   "sakila": set(["film_id", "actor_id"])}


""" Read SQL Database into python"""
class SQLData():
    def __init__(self, database_name):
        self.database_name = database_name
        self.mydb = mysql.connector.connect(host = "localhost", 
                                       user = "root", passwd = "book0722",
                                       database = self.database_name,
                                       auth_plugin ='mysql_native_password')
    
    
    def formatting(self, value):
        if value == None:
            return None
        
        if type(value) == decimal.Decimal:
            return str("{:,}".format(value))
        
        if type(value) == datetime.datetime:
            return str(value)
        
        return value
    
    
    def retrieveTable(self, table_name):
        mycursor = self.mydb.cursor()
        # column names
        # picked columns sakila database
        if self.database_name == "sakila":
            col_name = picked_cols[table_name]
        else:
            mycursor.execute("SHOW COLUMNS FROM {}".format(table_name))
            col_name = list(map(lambda x: x[0], mycursor.fetchall()))
            
        # special case for world database, country table
        if table_name == "country":
            col_name[0] = "CountryCode"
        
        # get data from SQL
        # special case for employees database
        if self.database_name == "employees" and table_name != "departments":
            mycursor.execute("SELECT * FROM {} WHERE EMP_NO <= 11000".format(table_name))
        # special case for employees database
        elif self.database_name == "sakila":
            picked = ", ".join(col_name)
            mycursor.execute("SELECT {} FROM {}".format(picked, table_name))
            
        else:
            mycursor.execute("SELECT * FROM {}".format(table_name))
        
        # store data
        data = list()
        # special case: merge two col to form pk
        if self.database_name == "world":
            # (1) for countrylanguage in world
            if table_name == "countrylanguage":
                for row in mycursor.fetchall():
                    modi_row = list(map(self.formatting, row))
                    pk = modi_row[0] + modi_row[1]
                    modi_row.append(pk.translate(str.maketrans('', '', string.punctuation)))
                    data.append(modi_row)
                col_name.append("CountryLanguage")
            else:
                for row in mycursor.fetchall():
                    data.append(list(map(self.formatting, row)))
            
        elif self.database_name == "employees":
            # (2) for titles in employees
            if table_name == "titles":
                for row in mycursor.fetchall():
                    modi_row = list(map(self.formatting, row))
                    pk = modi_row[0] + modi_row[2]
                    modi_row.append(pk)
                    data.append(modi_row)
                col_name.append("EmpDate")
            # (3) for dept_emp in employees
            elif table_name == "dept_emp":
                for row in mycursor.fetchall():
                    modi_row = list(map(self.formatting, row))
                    pk = modi_row[0] + modi_row[1]
                    modi_row.append(pk)
                    data.append(modi_row)
                col_name.append("EmpDept")
            else:
                for row in mycursor.fetchall():
                    data.append(list(map(self.formatting, row)))
        
        elif self.database_name == "sakila":
            # (4) for film_actor in sakila
            if table_name == "film_actor":
                for row in mycursor.fetchall():
                    modi_row = list(map(self.formatting, row))
                    pk = modi_row[0] + modi_row[1]
                    modi_row.append(pk)
                    data.append(modi_row)
                col_name.append("FilmActor")
            else:
                for row in mycursor.fetchall():
                    data.append(list(map(self.formatting, row)))
            
        else:
            for row in mycursor.fetchall():
                data.append(list(map(self.formatting, row)))
                
        return col_name, data
    
    
    
class FirebasePreparation():
    def __init__(self, db_name, database, primary_key, link_cols):
        self.db_name = db_name
        self.database = database
        self.url = "https://sql-searchdb.firebaseio.com/{}".format(db_name) # my path
        self._file_list = database.keys()
        self._search_tree = collections.defaultdict()
        self._link_tree = collections.defaultdict() 
        self._index = collections.defaultdict(list)
        self._primary_key = primary_key
        self._link_cols = link_cols
        
    
    # return keys that need to build as node
    def getKey(self, table, ref_type):
        if ref_type == "search":
            return self._primary_key[table]
        else:
            return self._link_cols[table]
    
    
    def formatting(self, value, col_name):
        if value == None:
            return None
        
        if col_name in avoid_nb_format[self.db_name]:
            return value
        
        if type(value) in [int, float]:
            return str("{:,}".format(value))
        
        return str(value)
    
    
    def __importAndParseFile(self, table):
        # for search tree, takes unique pk as node (need to combine two col if there is no pk)
        rows = collections.defaultdict(dict)
        # for link tree, which takes pk/fk as node, row(s) with that pk/fk as a list of children
        rows_forlink = collections.defaultdict(dict)
        
        key_cols = self.getKey(table, "link")
        for key_col in key_cols:
            rows_forlink[key_col] = collections.defaultdict(list)
        
        header = self.database[table]["column_names"]
        dataset = self.database[table]["data"]
            
        for line in dataset:
            # save records as a list of dictionary
            row = dict()
            for col, column_name in zip(line, header):
                row[column_name] = self.formatting(col, column_name)
                
            # build tree for linkage step
            for key_col in key_cols:
                key = row[key_col]
                rows_forlink[key_col][key].append(row)
                
            key = row[self.getKey(table, "search")]
            rows[key] = row
                
        self._search_tree[table] = rows
        self._link_tree[table] = rows_forlink
    

    def normalizedInput(self, words):
        # remove puctuation
        words = words.translate(str.maketrans('', '', string.punctuation))
        # split into list
        normalized_words = words.lower().split()
        return normalized_words


    def __createInvertIndexFromDictData(self):
        for table_name, rows in self._search_tree.items():
            for pk, row in rows.items():
                for col, data in row.items():
                    try: 
                        if data:
                            float(data)
                    except:
                        normalized_words = self.normalizedInput(data)
                        location = {'TABLE': table_name, 'COLUMN': col, "PK": pk}
                        for word in normalized_words:
                            self._index[word].append(location)
        
    
    def __uploadToFirebase(self):
        print("UPLOADING DATA ...")
        requests.patch(self.url + ".json", data = json.dumps(self._search_tree))
        requests.patch(self.url + "/link.json", data = json.dumps(self._link_tree))
        
        print("UPLOADING INVERTED INDEX ...")
        requests.patch(self.url + "/index.json", data = json.dumps(self._index))
        
        print("DONE")
        
    
    def main(self):
        for file in self._file_list:
            print("PREPROCESSING:", file, "...")
            self.__importAndParseFile(file)
        self.__createInvertIndexFromDictData()
        self.__uploadToFirebase()
        
        
if __name__ == "__main__":   
    database_name = sys.argv[1]
    firebase_root_name = sys.argv[2]
    SQLD = SQLData(database_name)
    
    database = collections.defaultdict(dict)
    for table in primary_key[database_name].keys():
        column_names, data = SQLD.retrieveTable(table)
        database[table]["column_names"] = column_names
        database[table]["data"] = data

    
    FPP = FirebasePreparation(database_name, database, primary_key[database_name], link_cols[database_name])
    FPP.main()