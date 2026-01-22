import time
import subprocess
from pymongo import MongoClient
import datetime
from pymongo import WriteConcern
from pymongo.read_preferences import ReadPreference


# CONFIGURARE
MONGO_SA_URI = "mongodb://localhost:27099/"
MONGO_SA_CONTAINER = "mongo_sa"
RS_NODES = [
    {"uri": "mongodb://localhost:27017/?directConnection=true", "container": "mongo_rs1"},
    {"uri": "mongodb://localhost:27018/?directConnection=true", "container": "mongo_rs2"},
    {"uri": "mongodb://localhost:27019/?directConnection=true", "container": "mongo_rs3"}
]
CONECTION_STRING_RS="mongodb://mongo_rs1:27017,mongo_rs2:27018,mongo_rs3:27019/?replicaSet=rs0"


def docker_control(container_name, action):
    print(f"   [DOCKER] {action.upper()} container: {container_name}...")
    subprocess.run(f"docker {action} {container_name}", shell=True, stdout=subprocess.DEVNULL)


def get_active_primary():
    for node in RS_NODES:
        client = None
        try:
            client = MongoClient(node["uri"], serverSelectionTimeoutMS=500,
            connectTimeoutMS=500
            )
            if client.admin.command('hello').get('isWritablePrimary'):
                return node
        except Exception:
            continue
        finally:
            if client: client.close()
    return None


# ==========================================
# TEST A: MONGO STANDALONE (Lipsa Toleranta-analog Oracle)
# ==========================================
def test_mongo_standalone():
    print("\n" + "="*60)
    print("TEST A: MONGODB STANDALONE (Single Instance)")
    print("Obiectiv: Demonstrarea LIPSEI tolerantei la partitionare.")
    print("="*60)

    #Check Initial
    try:
        client = MongoClient(
        MONGO_SA_URI, 
        serverSelectionTimeoutMS=500, 
        connectTimeoutMS=500, 
        )
        client.admin.command('ping')
        print("[STATUS] Conexiune initiala: ONLINE (OK)")
    except:
        print("[ERROR] Mongo Standalone nu ruleaza.")
        return
    
    #Simulare Cadere (Automat)
    print("[ACTIUNE] Opresc containerul Standalone...")
    docker_control(MONGO_SA_CONTAINER, "stop")
    time.sleep(2)

    #Verificare Downtime
    print("[TEST] Incercare conectare de ping...")
    try:
        client = MongoClient(
            MONGO_SA_URI,
            serverSelectionTimeoutMS=500,
            connectTimeoutMS=500,
        )
        client.admin.command('ping')
        print("[FAIL] Inca merge? Ceva e gresit.")
    except Exception:
        print("[SUCCESS] Conexiune Refuzata (Timeout)!")
        print("=> CONCLUZIE: MongoDB Standalone este INDISPONIBIL.")
    
# ==========================================
# TEST B: MONGO REPLICA SET (Toleranta Prezenta)
# ==========================================
def test_mongo_replicaset():
    print("\n" + "="*60)
    print("TEST B: MONGODB REPLICA SET / SHARDED")
    print("Obiectiv: Demonstrarea TOLERANTEI la partitionare (Failover).")
    print("="*60)

    #Identificare Primary
    primary_node = get_active_primary()
    if not primary_node:
        print("[ERROR] Clusterul pare jos complet.")
        return

    try:
        client = MongoClient(
        CONECTION_STRING_RS, 
        serverSelectionTimeoutMS=1000, 
        connectTimeoutMS=1000, 
        retryWrites=False
        )
        client.admin.command('ping')
        db = client.db_an3
        coll = db.doctors
        print("[STATUS] Conexiune initiala: ONLINE (OK)")
    except:
        print("[ERROR] Mongo Replica Set nu ruleaza.")
        return

    print(f"[INFO] Nodul Primary curent este: {primary_node['container']}")
    print("[STATUS] Cluster functional.")

    #Simulare Cadere (Kill Primary)
    print(f"[ACTIUNE] Opresc violent nodul Primary ({primary_node['container']})...")
    docker_control(primary_node['container'], "stop")

    #Monitorizare Failover (Election)
    print("[TEST] Monitorizez alegerea unui nou Primary (Election) si operatiile de scriere/citire...")
    start_time = time.time()
    recovered = False
    new_primary = None

    i=0
    while True:
        time.sleep(1)
        client = MongoClient(
        CONECTION_STRING_RS,
        serverSelectionTimeoutMS=1000,
        connectTimeoutMS=1000,
        retryWrites=False
        )
        db = client.db_an3
        coll = db.doctors
        i+=1
        print("Secunda "+str(i)+":")
        #Incercam sa scriem/citim

        try:
            coll.with_options(write_concern=WriteConcern(w='majority', wtimeout=100)).insert_one({
            "_id": 5002,
            "name": 'Mihai Telu',
            "hire_date": datetime.datetime.now(),
            "email": "mt@gmail.com",
            "specialties": []
            })
            print("A mers scrierea cu w=majority.")
        except:
            print("[SUCCESS] Scriere cu w=majority refuzata!")
            print("=> CONCLUZIE: MongoDB Replica Set este INDISPONIBIL pentru scriere.")

        try:
            coll.with_options(read_preference=ReadPreference.PRIMARY).find_one({"_id": 101})
            print("A mers citirea cu ReadPreference.PRIMARY.")
        except:
            print("[SUCCESS] Citire cu ReadPreference.PRIMARY refuzata!")
            print("=> CONCLUZIE: MongoDB Replica Set este INDISPONIBIL pentru citirea cu ReadPreference.PRIMARY .")

        try:
            coll.with_options(read_preference=ReadPreference.SECONDARY).find_one({"_id": 101})
            print("A mers citirea cu cel putin ReadPreference.SECONDARY.")
        except:
            print("[SUCCESS] Citire cu ReadPreference.SECONDARY refuzata!")
            print("=> CONCLUZIE: MongoDB Replica Set este INDISPONIBIL pentru citirea cu ReadPreference.SECONDARY .")

        current_primary = get_active_primary()
        if current_primary :
                new_primary = current_primary
                recovered = True
                break


    if recovered:
        duration = time.time() - start_time
        print(f"\n[SUCCESS] SISTEM RECUPERAT!")
        print(f"Noul Primary este: {new_primary['container']}")
        print(f"Timp de indisponibilitate: ~{duration:.2f} secunde.")
        print("=> CONCLUZIE: Clusterul functioneaza desi un nod este mort.")
        print("Aceasta este Toleranta la Partitionare (P din CAP).")
    else:
        print("\n[FAIL] Clusterul nu si-a revenit in timp util.")


if __name__ == "__main__":
    test_mongo_standalone()
    test_mongo_replicaset()

