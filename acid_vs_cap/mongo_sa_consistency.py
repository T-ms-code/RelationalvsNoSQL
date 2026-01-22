import threading
import time
from pymongo import MongoClient
import datetime


#CONFIGURARE
URI = "mongodb://localhost:27099/"
client = MongoClient(URI)
db = client.db_an3
coll = db.doctors


def setup_data():
    coll.delete_many({})
    #Initializam cu lista goala de specialitati
    coll.insert_one({
        "_id": 5000, 
        "name": 'Mihai Telu', 
        "hire_date": datetime.datetime.now(), 
        "email": "mt@gmail.com", 
        "specialties": [] 
    })
    print(" [INIT] Valoare initiala in DB: specialties=[]")


def update_doctor(thread_name, delay, spec_id, sp_name):
    #CITIRE (Read)
    doc = coll.find_one({"_id": 5000})
    lista_curenta = doc["specialties"]
    print(f" [{thread_name}] a citit: {lista_curenta}")
    
    #SIMULARE PROCESARE (Wait)
    #Fortam thread-ul sa astepte, ca celalalt sa apuce sa citeasca aceeasi lista veche
    time.sleep(delay)
    
    #MODIFICARE LOCALA (Modify)
    #Adaugam noua specialitate la lista (in memoria Python, nu in baza direct!)
    noua_lista = lista_curenta + [{"id": spec_id, "specialty": sp_name }]
    
    #SCRIERE INAPOI (Write) - AICI E PROBLEMA
    #Suprascriem tot campul 'specialties' cu versiunea noastra
    coll.update_one({"_id": 5000}, {"$set": {"specialties": noua_lista}})
    print(f" [{thread_name}] a scris: {noua_lista}")


def run_race_test():
    print("--- DEMONSTRATIE: MongoDB update prierdut/consistenta pierduta (Lipsa Izolare) ---")
    setup_data()

    #Thread A vrea sa adauge Cardiologie
    t1 = threading.Thread(target=update_doctor, args=("THREAD_A", 1, 15001, "Cardiologie"))
    #Thread B vrea sa adauge Neurologie
    t2 = threading.Thread(target=update_doctor, args=("THREAD_B", 1, 15002, "Neurologie"))
    
    #Pornim thread-urile
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    
    final_doc = coll.find_one({"_id": 5000})
    print(f"\n [FINAL] Valoare in DB: {final_doc['specialties']}")
    
    if len(final_doc['specialties']) == 1:
        print(" => CONCLUZIE: UPDATE PIERDUT! (Race Condition)")
        print("    Deoarece nu exista Lock, ambele au citit lista goala [].")
        print("    Fiecare a adaugat doar specializarea lui si a suprascris munca celuilalt.")
    else:
        print(" => Totul a mers corect (Surprinzator!).")


if __name__ == "__main__":
    run_race_test()