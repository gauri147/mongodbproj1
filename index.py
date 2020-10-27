# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 18:39:30 2020

@author: gauri, sumanth
"""
import mysql.connector
import json, decimal
import pymongo
from pymongo import MongoClient


#Establishing connection to MySQL
mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  password="password",
  database ="company_management"
)

#MongoDB Connection String
mongo_conn = "[mongodb connection]"
mongo_db = "Company_Management"
client = MongoClient(mongo_conn) 
#Database and Collections
db = client["Company_Management"]
proj_coll = db["project"]
emp_coll = db["employee"]

#Function Name: empty_collection
#Comments: If there are already any previous documents in the collection delete them
def empty_collection():
    x = proj_coll.delete_many({})
    x = emp_coll.delete_many({})
empty_collection()

#Function Name: Create_ProjectJson_DOC
#Comments: Extract data from MySQL and write it in JSON format into a text file.
def Create_ProjectJson_DOC():
    cursor = mydb.cursor()
    project_query =("SELECT Pnumber, Pname, Pnumber,Dname,Fname, Lname, Hours FROM project p JOIN department d ON p.Dnum = d.Dnumber JOIN works_on w ON w.Pno = p.Pnumber JOIN employee e ON e.Ssn = w.Essn")
    res=cursor.execute(project_query)
    row_headers=[x[0] for x in cursor.description]
    proj_result = cursor.fetchall()
    proj_result = [tuple(str(item) for item in t) for t in proj_result]

    proj_output = {} 
    for Pnumber, Pname, Pnumber,Dname,Fname, Lname, Hours in proj_result:
       if Pnumber in proj_output: 
            emp = {
                "Fname" : Fname,
                "Lname" : Lname,
                "Hours" : Hours
                }
            proj_output[Pnumber]["Employee"].append(emp) 
       else: 
            proj_output[Pnumber] =  {
                                "Pname" : Pname,
                                "Pnumber" : Pnumber,
                                "Dname" : Dname,
                                "Employee" : [{
                                    "Fname" : Fname,
                                    "Lname" : Lname,
                                    "Hours" : Hours
                                }]
                            }
    #print(proj_output)
    filename="project.json"
    proj_dict=[]
    for k, v in proj_output.items():
            proj_dict.append(v)

    project_data = json.dumps(proj_dict)
    with open(filename, 'w') as out:
       out.write(project_data + '\n')
    print("\n The data to be stored in project document collections is formed and is stored in file named project.json")
       
Create_ProjectJson_DOC()

#Function Name: Create_EmployeeJson_DOC
#Comments: Extract data from MySQL and write it in JSON format into a text file.
def Create_EmployeeJson_DOC():
    employee_query =("SELECT e.Ssn, e.Fname, e.Lname, p.Pname, p.Pnumber,  w.Hours, d.Dname FROM employee e JOIN works_on w ON e.Ssn = w.Essn JOIN project p ON p.Pnumber = w.Pno JOIN department d ON  d.Dnumber = e.Dno")
    cursor= mydb.cursor()
    res2=cursor.execute(employee_query)
    row_headers=[x[0] for x in cursor.description]
    emp_result = cursor.fetchall()
    emp_result = [tuple(str(item) for item in t) for t in emp_result]
    emp_output = {} 
    for Ssn, Fname, Lname, Pname, Pnumber, Hours, Dname in emp_result:
       if Ssn in emp_output: 
            proj = {
                "Pname" : Pname,
                "Pnumber" : Pnumber,
                "Dname" : Dname,
                }
            emp_output[Ssn]["Project"].append(proj) 
       else: 
            emp_output[Ssn] =  {
                                "Fname" : Fname,
                                "Lname" : Lname,
                                "Hours" : Hours,
                                "Project" : [{
                                     "Pname" : Pname,
                                     "Pnumber" : Pnumber,
                                     "Dname" : Dname
                                }]
                            }

    filename="employee.json"
    emp_dict=[]
    for k, v in emp_output.items():
            emp_dict.append(v)

    employee_data = json.dumps(emp_dict)
    with open(filename, 'w') as out:
       out.write(employee_data + '\n')
    print("\n The data to be stored in employee document collections is formed and is stored in file named employee.json")
    
   
       
Create_EmployeeJson_DOC()

#Function Name: Import_EmployeeDoc_MDB
#Comments: Create a collection and import your data into it
def Import_EmployeeDoc_MDB():
    with open('employee.json') as file: 
        file_data = json.load(file)
    if isinstance(file_data, list): 
        emp_coll.insert_many(file_data)   
    else: 
        emp_coll.insert_one(file_data)
    print("\n Data imported into employee collection")
Import_EmployeeDoc_MDB()
       
#Function Name: Import_ProjectDoc_MDB
#Comments: Create a collection and import your data into it
def Import_ProjectDoc_MDB():
    with open('project.json') as file1: 
        file_data1 = json.load(file1)
    #print(file_data1)
    if isinstance(file_data1, list): 
        proj_coll.insert_many(file_data1)   
    else: 
        proj_coll.insert_one(file_data1)
    print("\n Data imported into project collection")
Import_ProjectDoc_MDB()

print("\n ---------- Task4 : Queries --------")
#Task4 - Queries 
#Query1(find() method in mongo DB)
#Find from employee document having Fname = Jennifer
print("\n Query: Find from employee document having Fname = Jennifer")
query1 =  {"Fname" : "Jennifer"}
mydoc = emp_coll.find(query1)
for x in mydoc:
   print(x)
   
#Query 2 (regex method)
#Find from project document having Pname starting with P
print("\n Query: Find from project document having Pname starting with P")
query2 = {"Pname" : {"$regex" : "^P"}} 
mydoc = proj_coll.find(query2)
for x in mydoc:
    print(x)

#Query 3 
#Sort the project data using the Pnumber in Ascending format
print("\n Query: Sort the project data using the Pnumber in Ascending format")
query3 = proj_coll.find().sort([("Pnumber", pymongo.ASCENDING)]).limit(3)
for x in query3:
   print(x)

#Query 4
#Delete from project document having Pname = ProductX
print("\n Query: Delete from project document having Pname = ProductX")
query4= {"Pname" : "ProductX"}
count = proj_coll.find(query4).count()
print("Before: Count with Pname = ProductX: {}".format(count))
print("Executing deletion......")
proj_coll.delete_one(query4)
count = proj_coll.find(query4).count()
print("After deletion: Count with Pname = ProductX: {}".format(count))

#----------------------- BONUS TASK ---------------------------
#BONUS points Task 1: Convert into XML format  

#For Employee Document
#Function Name: employee_jsontoxml
#Comments: Fetch data from db and write the output in XML format
def employee_jsontoxml():
    import json as j
    import dicttoxml

    with open("employee.json") as jff: 
       d = j.load(jff)
    
    
    xml = dicttoxml.dicttoxml(d, custom_root='employee')
    xml1 =xml.decode('utf-8')
    print("\n Employee document to XML (Written in employee.xml)")
    print(xml1)
    print("\n")
    
    filename = "employee_xml.xml"
    with open(filename, 'w') as out:
       out.write(str(xml1) + '\n')
        
employee_jsontoxml()


#For Project Document
#Function Name: project_jsontoxml
#Comments: Fetch data from db and write the output in XML format
def project_jsontoxml():
    import json as j
    import dicttoxml
    
    with open("project.json") as jf:
        s= j.load(jf)
        
    xml =dicttoxml.dicttoxml(s,custom_root = "project")
    xml2 =xml.decode('utf-8')
    print("\n Project document to XML (Written in project.xml)")
    print(xml2)
    print("\n")
    
    filename = "project_xml.xml"
    with open(filename, 'w') as out:
       out.write(str(xml2) + '\n')

project_jsontoxml()
    
                                            
#BONUS points Task 2: Create department document          
def Create_DepartmentJson_DOC():
    department_query =("SELECT m.Dname,m.Dnumber,m.Lname AS Mgr_lname,m.Fname AS Mgr_fname, emp.Lname, emp.Fname, emp.Salary  FROM Employee emp JOIN (SELECT * FROM department d JOIN employee e WHERE d.Mgr_ssn = e.Ssn AND d.Dnumber = e.Dno) AS m WHERE emp.Super_ssn = m.ssn AND emp.Dno = m.DNumber")
    cursor= mydb.cursor()
    res2=cursor.execute(department_query)
    row_headers=[x[0] for x in cursor.description]
    dept_result = cursor.fetchall()
    dept_result = [tuple(str(item) for item in t) for t in dept_result]
    #print(dept_result)
    dept_output = {} 
    for Dname,Dnumber, Mgr_lname, Mgr_fname, Lname, Fname, Salary, in dept_result:
       if Dname in dept_output: 
            emp = {
                "Lname" : Lname,
                "Fname" : Fname,
                "Salary" : Salary,
                }
            dept_output[Dname]["employee"].append(emp) 
       else: 
            dept_output[Dname] =  {
                                "Dname" : Dname,
                                "Dnumber" : Dnumber,
                                "Mgr_lname" : Mgr_lname,
                                "Mgr_fname" : Mgr_fname,
                                "employee" : [{
                                     "Lname": Lname,
                                     "Fname" : Fname,
                                     "Salary" : Salary
                                }]
                            }

    #print(dept_output)
    dept_data = json.dumps(dept_output)
    #print(dept_data)
    
    filename="dept.json"
    with open(filename, 'w') as out:
       out.write(dept_data + '\n')
       
Create_DepartmentJson_DOC()



        








  
