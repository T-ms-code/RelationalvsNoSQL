import time
import datetime
from pymongo import MongoClient
from pymongo import WriteConcern
from pymongo.read_preferences import ReadPreference


#CONFIGURARE
DB_NAME = "db_an3"
COLL_NAME = "patients"
uri = "mongodb://mongo_rs1:27017,mongo_rs2:27018,mongo_rs3:27019/?replicaSet=rs0"

def test_strong_consistency(uri):
    print("\n" + "="*60)
    print("SCENARIUL 1: STRONG CONSISTENCY (Similar ACID)")
    print("Config: WriteConcern='majority', ReadPreference='primary'")
    print("="*60)

    #Conectare
    client = MongoClient(uri)
    db = client[DB_NAME]

    coll = db.get_collection(COLL_NAME)
    start_time = time.time()
    
    #Executam operatii
    for i in range(1000):
        #Scrierea va bloca executia pana cand datele sunt replicate pe majoritate
        coll.with_options(write_concern=WriteConcern(w="majority")).insert_one({"_id": 200+i, "name": "Patient"+str(i), "born_date":datetime.datetime.now(), "email": f"pat{i}@gmail.com", "treatments": []})
        
        #Citirea se face de pe Primary, deci datele sunt garantate sa fie acolo
        doc = coll.with_options(read_preference=ReadPreference.PRIMARY).find_one({"_id": 200+i})
        if doc is None:
            print("Inconsistenta!!!")

    duration = time.time() - start_time
    print(f"\nREZULTAT: Timp total (scrieri lente, sigure): {duration:.4f} secunde")
    print("Concluzie: Datele sunt consistente.")
    client.close()


def test_eventual_consistency(uri):
    print("\n" + "="*60)
    print("SCENARIUL 2: EVENTUAL CONSISTENCY (Replication Lag)")
    print("Config: WriteConcern=1, ReadPreference='secondary'")
    print("="*60)

    #2 conexiuni separate pentru a simula corect
    client_writer = MongoClient(uri)
    client_reader = MongoClient(uri)

    #Scriitorul: Scrie rapid pe Primary (Confirmare doar de la 1 nod=Primary/el insusi)
    coll_writer = client_writer[DB_NAME].get_collection(COLL_NAME)

    #Cititorul: Citeste fortat de pe Secondary
    coll_reader = client_reader[DB_NAME].get_collection(COLL_NAME)

    start_time = time.time()
    lag_count = 0

    for i in range(1000):
        #Scriere Rapida
        coll_writer.with_options(write_concern=WriteConcern(w=1)).insert_one({"_id": 1300+i, "name": "Patient"+str(i), "born_date":datetime.datetime.now(), "email": f"pat{i}@gmail.com", "treatments": []})
        
        #Citire Imediata de pe celalalt nod
        doc = coll_reader.with_options(read_preference=ReadPreference.SECONDARY).find_one({"_id": 1300+i})
        
        if doc is None:
            lag_count += 1

    duration = time.time() - start_time
    print(f"\nREZULTAT: Lag detectat in {lag_count} din 1000 cazuri; Timp total (scrieri/citiri rapide, nesigure): {duration:.4f} secunde")
    
    
    if lag_count > 0:
        print("Concluzie: S-a demonstrat 'Eventual Consistency'.") 
        print("Clientul a citit date vechi (sau null) de pe Secondary imediat dupa scriere.")
    else:
        print("Nota: Pe localhost (Docker), replicarea este extrem de rapida (<1ms).")
        print("Intr-o retea reala, acest numar ar fi fost semnificativ mai mare.")

    client_writer.close()
    client_reader.close()


if __name__ == "__main__":
    #Rulam Testul1
    test_strong_consistency(uri)
    
    #Rulam Testul2
    test_eventual_consistency(uri)