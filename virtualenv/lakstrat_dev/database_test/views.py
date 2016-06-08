from django.shortcuts import render
from django.http import HttpResponse
from django.db import connections
from django.db import connection
# Create your views here.


def index(request):
    res = "row count is " + my_custom_sql()
    return HttpResponse(res)


def my_custom_sql():
     
    #cursor = connections[RDS_DB_NAME].cursor()
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) from tbl_habitat")
    row = cursor.fetchall()
    return str(row)

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]