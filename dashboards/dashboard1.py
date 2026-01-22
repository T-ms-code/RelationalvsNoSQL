import streamlit as st
import pandas as pd
import plotly.express as px
import oracledb
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


# ==========================================
# CONFIGURARE CONEXIUNI
# ==========================================
# Oracle
ORACLE_USER = "db_an3"
ORACLE_PASS = "parola123"
ORACLE_DSN  = "localhost:1521/mihaibase"

#MongoDB URIs
MONGO_SA_URI = "mongodb://localhost:27099/"
MONGO_RS_URI = "mongodb://mongo_rs1:27017,mongo_rs2:27018,mongo_rs3:27019/?replicaSet=rs0"
MONGO_SHARD_URI = "mongodb://localhost:27067/"

#Configurare Pagina Streamlit
st.set_page_config(page_title="BD Dashboard: Relational vs NoSQL", layout="wide", page_icon="📊")

st.title("Monitorizare Arhitecturi Baze de Date")
st.markdown("Compararea distribuției datelor și stării arhitecturii pentru **Oracle vs MongoDB (Standalone, ReplicaSet, Sharded)**.")

#Buton de refresh
if st.button('Actualizează Datele'):
    st.rerun()


# ==========================================
# FUNCTII DE DATA FETCHING
# ==========================================

def get_oracle_data():
    try:
        conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN)
        cursor = conn.cursor()
        
        #Luam tabelele create de noi
        tables = ["DOCTOR", "PATIENT", "SPECIALTY", "TREATMENT", "HAS"]
        data = []
        for t in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            count = cursor.fetchone()[0]
            
            #Estimam marimea 
            cursor.execute(f"SELECT num_rows, avg_row_len FROM user_tables WHERE table_name = '{t}'")
            res = cursor.fetchone()
            size_mb = 0
            if res and res[0] and res[1]:
                size_mb = (res[0] * res[1]) / (1024**2) #Convertim la MB
            
            data.append({"Tabel": t, "Randuri": count, "Marime (MB)": round(size_mb, 4)})
        
        conn.close()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Eroare Oracle: {e}")
        return None


def get_mongo_sa_stats():
    try:
        client = MongoClient(MONGO_SA_URI, serverSelectionTimeoutMS=2000)
        db = client.db_an3
        stats = db.command("dbStats")
        
        doc_counts = {
            "Doctors": db.doctors.count_documents({}),
            "Patients": db.patients.count_documents({})
        }
        client.close()
        return stats, doc_counts##-- !!!numele bazei, dimensiunea, colectiile(nr. lor) etc.
    except Exception as e:
        st.error(f"Eroare Mongo StandAlone: {e}")
        return None, None


def get_mongo_rs_status():
    try:
        #Ne conectam la Primary 
        client = MongoClient(MONGO_RS_URI, serverSelectionTimeoutMS=2000)
        
        #Status Replica Set (Membri, Stare, Last replication)
        status = client.admin.command("replSetGetStatus")##--!!!gasim numele membrilor,  starea(primar/secundar), sanatatea(oprit/pornit), ultima replicare
        
        #Config Replica Set (Timeouts, Settings)
        config = client.admin.command("replSetGetConfig")##--!!!gasim id-ul fiecarui membru, prioriatea, voturile,  Election Timeout, Heartbeat Interval, Heartbeat Timeout. 
        
        client.close()
        return status, config['config']
    except Exception as e:
        st.error(f"Eroare Replica Set: {e}")
        return None, None


def get_mongo_sharded_stats():
    data = {}
    try:
        client = MongoClient(MONGO_SHARD_URI, serverSelectionTimeoutMS=2000)
        
        #Ping & Connection
        try:
            client.admin.command('ping')
            data['connected'] = True ##!!! daca suntem conectati sau nu
        except ConnectionFailure:
            data['connected'] = False
            return data

        #List Shards
        shard_status = client.admin.command("listShards")
        data['shards_info'] = shard_status.get('shards', [])##!!!id-ul, haost-ul si starea shard-urile de pe intreg clientului care contine baza
        
        #Check Config Database(INTERN)
        config_db = client.config
        #Filtram doar baza noastra 'db_an3'
        db_config_doc = config_db.databases.find_one({"_id": "db_an3"})
        data['db_config'] = db_config_doc## -- !!!id-ul bazei, partitionare, shard-uri(primare/secundare)

        #DB Stats Global 
        db = client.db_an3
        data['db_stats'] = db.command("dbStats")##-- !!!numele bazei, dimensiunea, colectiile

        #Collection Stats & Distribution 
        #Verificam distributia pe colectia 'patients'
        try:
            coll_stats = db.command("collStats", "patients")
            data['coll_stats'] = coll_stats##-- !!!totatul documentelor, toataul dimensiunii si daca e shard-uita colectia patients din baza noastra 
        except OperationFailure:##-- !!!daca e sharded, gasim numele shard-ului, numarul documentelor, dimensiunea  si procentul(din toate documetele din colectie) de pe fiecare shard
            data['coll_stats'] = None #Colectia poate nu exista inca

        client.close()
        return data

    except Exception as e:
        st.error(f"Eroare Sharding Logic: {e}")
        return None


# ==========================================
# INTERFATA VIZUALA (TABS)
# ==========================================
tab1, tab2, tab3, tab4 = st.tabs(["Oracle Relational", "Mongo Standalone", "Mongo Replica Set", "Mongo Sharded Cluster"])

# --- TAB 1: ORACLE ---
with tab1:
    st.header("Arhitectura Monolitică Relațională")
    df_oracle = get_oracle_data()
    
    if df_oracle is not None:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.metric("Total Tabele", len(df_oracle))
            total_rows = df_oracle["Randuri"].sum()
            st.metric("Total Rânduri", total_rows)
            st.metric("Dimensiune totala (MB)", df_oracle["Marime (MB)"].sum())
        
        with col2:
            st.dataframe(df_oracle, use_container_width=True)
            
            #Grafic simplu
            fig = px.bar(df_oracle, x='Tabel', y='Randuri', title="Număr de înregistrări per Tabel", color='Tabel')
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Oracle Database este offline sau inaccesibilă.")


# --- TAB 2: MONGO STANDALONE ---
with tab2:
    st.header("Arhitectura NoSQL Standalone")
    stats_sa, counts_sa = get_mongo_sa_stats()
    
    if stats_sa:
        c1, c2, c3 = st.columns(3)
        size_mb = stats_sa['dataSize'] / (1024**2)
        c1.metric("Dimensiunea bazei", f"{size_mb:.4f} MB")
        c2.metric("Numarul colectiilor", stats_sa['collections'])
        c3.metric("Numarul obiectelor", stats_sa['objects'])
        
        st.subheader("Documente per Colecție")
        st.json(counts_sa)
    else:
        st.warning("MongoDB Standalone este offline.")


# --- TAB 3: MONGO REPLICA SET ---
with tab3:
    st.header("Arhitectura High Availability (Replica Set)")
    rs_status, rs_config = get_mongo_rs_status()
    
    if rs_status and rs_config:
        #Configurare Tehnica
        st.subheader("Parametri de configurare")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Numele setului", rs_status['set'])
        c2.metric("Election Timeout", f"{rs_config['settings']['electionTimeoutMillis']} ms")
        c3.metric("Heartbeat Interval", f"{rs_config['settings']['heartbeatIntervalMillis']} ms")
        c4.metric("Heartbeat Timeout", f"{rs_config['settings']['heartbeatTimeoutSecs']} s")

        
        #Membri si Stare
        st.subheader("Statusurile nodurilor")
        
        members_data = []
        for m in rs_status['members']:
            #Calculam lag-ul (diferenta de timp fata de 'now')
            last_heartbeat = str(m.get('lastHeartbeat', 'N/A'))
            
            members_data.append({
                "ID": m['_id'],
                "Host": m['name'],
                "Stare": m['stateStr'],  # PRIMARY / SECONDARY
                "Sanatate": "OK" if m['health'] == 1 else "Down",
                "Uptime (s)": m.get('uptime', 0),
                "Voturi": m.get('votes', 1),
                "Prioritate": m.get('priority', 1),
                "Last heartbeat": last_heartbeat
            })
            
        df_rs = pd.DataFrame(members_data)
        
        #Colorare conditionata pentru tabel
        def highlight_primary(row):
            return ['background-color: #d4edda' if row['Stare'] == 'PRIMARY' else '' for _ in row]

        st.dataframe(df_rs.style.apply(highlight_primary, axis=1), use_container_width=True)
    else:
        st.warning("MongoDB Replica Set este offline sau inaccesibil.")


# --- TAB 4: MONGO SHARDED CLUSTER ---
with tab4:
    st.header("Arhitectura Scalabilă Orizontal (Sharding)")
    
    #Fetch Data folosind functia adaptata
    sh_data = get_mongo_sharded_stats()
    
    if sh_data and sh_data.get('connected'):
        
        #SHARDED CLUSTER INFORMATION (listShards)
        st.subheader("Informatii despre Sharded Cluster")
        st.info(f"Numărul total de shard-uri active: {len(sh_data['shards_info'])}")
        
        cols = st.columns(len(sh_data['shards_info']))
        for idx, shard in enumerate(sh_data['shards_info']):
            with cols[idx]:
                st.markdown(f"**Shard: {shard['_id']}**")
                st.caption(f"Host: {shard['host']}")
                st.caption(f"Stare: {shard.get('state', 'unknown')}")
                
        st.divider()

        # DATABASE & CONFIG STATUS (config.databases)
        st.subheader("Statusul curent de sharding pentru baza db_an3")
        
        db_conf = sh_data.get('db_config')
        if db_conf:
            c1, c2 = st.columns(2)
            c1.metric("ID bazei", db_conf['_id'])
            #c2.metric("Partionata", "DA" if db_conf.get('partitioned') else "NU") ##SE ACUTUALIZEAZA GREU, ASA CA-L SCOT
            c2.metric("Primary Shard", db_conf.get('primary', 'N/A'))
            
            # Global Size
            global_size_mb = sh_data['db_stats'].get('dataSize', 0) / (1024**2)
            nr_documents = sh_data['db_stats'].get('collections', 0) 
            
            st.divider()
            col_size, col_docs = st.columns(2)
            col_size.metric("Dimensiunea totală a bazei (Global)", f"{global_size_mb:.4f} MB")
            col_docs.metric("Nr. total de colecții/obiecte (inclusiv interne createv de Mongo)", nr_documents)
        else:
            st.warning("Baza de date 'db_an3' nu apare în config.databases (posibil încă ne-sharduită).")

        st.divider()

        # CHECK DISTRIBUTION ACROSS SHARDS (collStats)
        st.subheader("Distributia datelor in shard-uri pentru colectia patients")
        
        coll_stats = sh_data.get('coll_stats')
        
        if coll_stats:
            total_count = coll_stats.get('count', 0)
            total_size_mb = coll_stats.get('size', 0) / (1024**2)
            is_sharded = coll_stats.get('sharded', False)
            
            st.markdown(f"**Totalul documentelor:** {total_count:,}")
            st.markdown(f"**Marimea colectiei:** {total_size_mb:.4f} MB")
            
            if is_sharded:
                shards_dict = coll_stats.get('shards', {})
                
                rows = []
                for shard_name, s_stats in shards_dict.items():
                    cnt = s_stats.get('count', 0)
                    sz_mb = s_stats.get('size', 0) / (1024**2)
                    pct = (cnt / total_count * 100) if total_count > 0 else 0
                    
                    rows.append({
                        "Shard": shard_name,
                        "Documente": cnt,
                        "Dimensiune (MB)": round(sz_mb, 4),
                        "Procent pe shard din documente(%)": round(pct, 2)
                    })
                
                df_dist = pd.DataFrame(rows)
                
                # Afisare Tabel
                st.dataframe(df_dist, use_container_width=True)
                
                # Vizualizare Grafica
                st.markdown("### Visual Distribution")
                
                # CORECTIE AICI: Numele coloanei trebuie sa coincida cu cel din DataFrame
                # Bar Chart Interactiv
                fig_bar = px.bar(df_dist, x='Shard', y='Procent pe shard din documente(%)', 
                                 text='Procent pe shard din documente(%)', color='Shard',
                                 title="Distribuția Procentuală a Documentelor",
                                 range_y=[0, 100])
                st.plotly_chart(fig_bar, use_container_width=True)
                
                # Pie Chart pentru volum
                fig_pie = px.pie(df_dist, values='Documente', names='Shard', 
                                 title="Distribuție Absolută (Documente)", hole=0.4)
                st.plotly_chart(fig_pie, use_container_width=True)
                
            else:
                st.warning("Colecția 'patients' există, dar NU apare ca 'sharded' în collStats.")
        else:
            st.error("Nu s-au putut obține statistici pentru colecția 'patients' (posibil inexistentă).")
            
    else:
        st.error("Server Sharded Cluster not available (Verifică conexiunea la portul 27067)")

# Footer
st.markdown("---")
st.caption("Proiect Baze de Date Avansate - Dashboard generat automat cu Python & Streamlit")