"""
1. Show the list of first and last names of the employees from London.
2. Show the list of first and last names of the employees whose first name begins with letter A.
3. Show the list of first, last names and ages of the employees whose age is greater than 55. The result should be sorted by last name.
4. Calculate the greatest, the smallest and the average age among the employees from London.
5. Show the list of cities in which the average age of employees is greater than 60 (the average age is also to be shown)
6. Show first, last names and ages of 3 eldest employees.

"""

import psycopg2
from psycopg2 import Error
from settings import *


def fprint(data):
    print("Result:")
    for i in range(len(data)):
        print(data[i])
    print('----------------------------------------------------------------')


try:
    connection = psycopg2.connect(user=USER,
                                  password=PASSWORD,
                                  host=HOST,
                                  port=PORT,
                                  database='shop_db')
    cursor = connection.cursor()
    date = 'extract (year from now()) -extract(year from date_of_birds)'
    # 1. Show the list of first and last names of the employees from London.
    cursor.execute(
        "SELECT first_name,last_name from employee where city_id in (select id from city c where c.city_name='London')")
    fprint(cursor.fetchall())
    # 2. Show the list of first and last names of the employees whose first name begins with letter A(N).
    cursor.execute(
        "Select first_name,last_name from employee where first_name  like 'N%'")
    fprint(cursor.fetchall())
    # 3. Show the list of first, last names and ages of the employees whose age is greater than 55. The result should be sorted by last name.
    cursor.execute(
        f"""Select first_name,last_name from employee
           where ({date})  >55
           order by last_name""")
    fprint(cursor.fetchall())
    # 4. Calculate the greatest, the smallest and the average age among the employees from London.

    cursor.execute(
        f"""
        Select  min({date}),max({date}),round(avg({date})) from employee
        where city_id in
        (select id from city c where c.city_name='London')
        """)
    fprint(cursor.fetchall())
    # 5. Show the list of cities in which the average age of employees is greater than 60(50) (the average age is also to be shown)
    cursor.execute(
        f"""
        select city_name,round(avg({date})) from city c
        left join employee e on c.id =e.city_id  group by city_name having round(avg({date}))>=50

        """)
    fprint(cursor.fetchall())
    # 6. Show first, last names and ages of 3 eldest employees.
    cursor.execute(
        f"""
        Select first_name,last_name,{date} as age from employee e 
        order by age desc limit 3
        """)
    fprint(cursor.fetchall())

except (Exception, Error) as error:
    print("Error while working with PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection closed")
