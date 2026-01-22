import streamlit as st
import pandas as pd
import plotly.express as px
import time
import oracledb
import pymongo
from concurrent.futures import ThreadPoolExecutor, as_completed
import random


ORACLE_CONFIG = {
    "user": "db_an3",
    "password": "parola123",
    "dsn": "localhost:1521/mihaibase" ,
    "tcp_connect_timeout": 1
}

MONGO_URIS = {
    "Mongo Standalone": "mongodb://localhost:27099/",
    "Mongo ReplicaSet": "mongodb://mongo_rs1:27017,mongo_rs2:27018,mongo_rs3:27019/?replicaSet=rs0",
    "Mongo Sharded": "mongodb://localhost:27067/"
}

DB_NAME = "db_an3"


def run_simple_oracle(conn):
    cursor = conn.cursor()
    id = random.randint(5002, 7001)
    cursor.execute("SELECT * FROM patient WHERE patient_id < :id AND email LIKE '%yahoo%'", [id])
    res = cursor.fetchone() 
    cursor.close()
    return res


def run_simple_mongo(db):
    id = random.randint(5002, 7001)
    res = db.patients.find_one({
        "_id": {"$lt": id}, 
        "email": {"$regex": "yahoo", "$options": "i"}
    })
    return res


def run_agg_oracle(conn):
    cursor = conn.cursor()
    id = random.randint(0, 14)
    query = """
        SELECT s.name, COUNT(t.treatment_id) AS total
        FROM specialty s
        JOIN treatment t ON t.specialty_id = s.specialty_id
        WHERE t.specialty_id > :id 
        GROUP BY s.name
        ORDER BY total DESC
    """
    cursor.execute(query, [id])
    res = cursor.fetchall()
    cursor.close()
    return res

def run_agg_mongo(db):
    id = random.randint(0, 14)
    pipeline = [
        {"$unwind": "$treatments"}, 
        {"$match": {
            "treatments.specialty_id": {"$gt": id}
        }},
        {"$group": {
            "_id": "$treatments.specialty_name", 
            "total": {"$sum": 1}
        }},
        {"$sort": {"total": -1}}
    ]
    res = list(db.patients.aggregate(pipeline))
    return res

def get_oracle_connection():
    try:
        return oracledb.connect(**ORACLE_CONFIG)
    except Exception as e:
        return None


def get_mongo_database(uri):
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=100)
        client.server_info() #Trigger connection check
        return client[DB_NAME]
    except Exception as e:
        return None


def benchmark_instance(name, type):
    results = {}
    
    conn = None
    db = None
    
    if type == "oracle":
        conn = get_oracle_connection()
        if not conn: return None
        exec_simple = lambda: run_simple_oracle(conn)
        exec_agg = lambda: run_agg_oracle(conn)
    else:
        db = get_mongo_database(MONGO_URIS[name])
        if db is None: return None
        exec_simple = lambda: run_simple_mongo(db)
        exec_agg = lambda: run_agg_mongo(db)

    #LATENCY
    latencies_simple = []
    for _ in range(50):
        start = time.time()
        exec_simple()
        latencies_simple.append((time.time() - start) * 1000) #ms
    
    latencies_agg = []
    for _ in range(10):# Agregatele sunt mai grele, rulam mai putine
        start = time.time()
        exec_agg()
        latencies_agg.append((time.time() - start) * 1000) #ms

    results['Latency Simple (ms)'] = sum(latencies_simple) / len(latencies_simple)
    results['Latency Agg (ms)'] = sum(latencies_agg) / len(latencies_agg)

    #THROUGHPUT (Executii in 3 secunde - Simple Query)
    start_time = time.time()
    count = 0
    while time.time() - start_time < 3:
        exec_simple()
        count += 1
    results['Throughput Simple (req/sec)'] = count / 3

    start_time = time.time()
    count = 0
    while time.time() - start_time < 3:
        exec_agg()
        count += 1
    results['Throughput Agg (req/sec)'] = count / 3

    #AVAILABILITY UNDER STRESS (Concurrent Users)
    total_req = 10000
    
    def task_simple():
        try:
            exec_simple()
            return True
        except:
            return False
        
    def task_agg():
        try:
            exec_agg()
            return True
        except:
            return False

    success = 0
    with ThreadPoolExecutor(max_workers=100) as executor:##PROCESAM cate 100 de cereri din cele 10000
        futures = [executor.submit(task_simple) for _ in range(total_req)]
        for f in as_completed(futures):
            if f.result():
                success += 1
    results['Availability Simple (%)'] = (success / total_req) * 100

    success = 0
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(task_agg) for _ in range(total_req)]
        for f in as_completed(futures):
            if f.result():
                success += 1
    results['Availability Agg (%)'] = (success / total_req) * 100

    if conn: conn.close()
    if db is not None: db.client.close()
    return results


#INTERFATA STREAMLIT
st.set_page_config(page_title="Performanta bazelor", layout="wide")
st.title("Dashboard de performanta")
st.markdown("Compararea performanței între **Oracle (Relational)** și **MongoDB (NoSQL)** pe 3 metrici cheie.")

if st.button("RULEAZĂ TESTELE DE PERFORMANȚĂ (Poate dura 1-2 min)"):
    with st.spinner('Rulare benchmark-uri... Te rog așteaptă.'):
        
        data = []
        
        #Oracle
        res = benchmark_instance("Oracle Database", "oracle")
        if res:
            res['System'] = "Oracle 19c"#Adugam si denumirea sistemului la vectorul sau de caracteristici
            data.append(res)
        else:
            st.error("Oracle nu este accesibil.")

        #MongoDB Instances
        for name in MONGO_URIS.keys():
            res = benchmark_instance(name, "mongo")
            if res:
                res['System'] = name
                data.append(res)
            else:
                st.warning(f"{name} nu este accesibil.")

        df = pd.DataFrame(data)

        # Afisare Rezultate
        st.success("Teste finalizate!")
        
        # Grafice
        c1, c2 = st.columns(2)
        
        with c1:
            st.subheader("Timp de răspuns/Latență")
            st.caption("Mai mic este mai bine (Milisecunde)")
            fig_lat = px.bar(df, x='System', y=['Latency Simple (ms)', 'Latency Agg (ms)'], 
                             barmode='group', title="Latency: Simple vs Aggregate Query")
            st.plotly_chart(fig_lat, use_container_width=True)

        with c2:
            st.subheader("Throughput")
            st.caption("Mai mare este mai bine. (Cereri/Secundă)")
            fig_thr = px.bar(df, x='System', y=['Throughput Simple (req/sec)', 'Throughput Agg (req/sec)'], 
                            color_discrete_sequence=['#00CC96', '#EF553B'], title="Throughput Capacity: Simple vs Aggregate Query")
            st.plotly_chart(fig_thr, use_container_width=True)

        st.subheader("Availability under Stress")
        st.caption("Rata de succes la 100 useri concurenți (Simulare)")
        fig_avail = px.bar(df, x='System', 
                               y=['Availability Simple (%)', 'Availability Agg (%)'], 
                               barmode='group',
                               range_y=[0, 110], #Fixam axa Y la 100% ca sa vedem clar scaderile
                               color_discrete_sequence=['#00CC96', '#EF553B']) #Culori custom (Verde/Rosu)
        st.plotly_chart(fig_avail, use_container_width=True)

        st.subheader("Date Brute")
        st.dataframe(df)

else:
    st.info("Apasă butonul de mai sus pentru a începe benchmark-ul live.")