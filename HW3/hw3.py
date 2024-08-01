#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2


# In[21]:


connection = psycopg2.connect(user="cs143", password="cs143", host="localhost", port="5432", database="cs143")
connection.autocommit=True
with connection.cursor() as cur:
    cur.execute("""
    SELECT sf_trip_start.id AS trip_id, sf_trip_user.user_type AS user_type,
    CASE
    WHEN sf_trip_user.user_type = 'Subscriber' THEN COALESCE(TRUNC(0.2 * EXTRACT(EPOCH FROM(sf_trip_end.time - sf_trip_start.time))/60, 2), 1000)
    ELSE COALESCE(3.49 + TRUNC(0.3*EXTRACT(EPOCH FROM(sf_trip_end.time - sf_trip_start.time))/60, 2), 1000)
    END AS trip_charge
    FROM sf_trip_start
    LEFT JOIN sf_trip_end
    LEFT JOIN sf_trip_user
    ON sf_trip_end.id = sf_trip_user.trip_id
    ON sf_trip_start.id = sf_trip_end.id
    ORDER BY trip_id;
    """)
    rows=cur.fetchall()
    print("SAN FRANCISCO BIKE SHARE \nRoster of Charges \nTrip ID          User Type          Charge\n-------------------------------------------------")
    for result in rows:
        trip_id, user_type, trip_charge = result
        print("{}             {}         ${}".format(trip_id, user_type, trip_charge))
connection.close()


# In[ ]:




