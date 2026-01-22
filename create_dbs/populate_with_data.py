import random
import oracledb
from faker import Faker
# IMPORT CORRECT PENTRU EVAL:
import datetime #MODUL
from datetime import timedelta#CLASA


from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import random
import ast


import os
from datetime import date # Daca sunt si obiecte date simple


#CONFIGURARE CONEXIUNE
DB_USER = "db_an3"
DB_PASS = "parola123"
DB_DNS  = "localhost:1521/mihaibase" 
CONNECTION_STRING1 = "mongodb://localhost:27099/"
CONNECTION_STRING2 = "mongodb://mongo_rs1:27017,mongo_rs2:27018,mongo_rs3:27019/?replicaSet=rs0"
CONNECTION_STRING3 = "mongodb://localhost:27067/"


#PARAMETRI GENERARE
NUM_DOCTORS = 50
NUM_PATIENTS = 2000
MAX_TREATMENTS_PER_PATIENT = 5

fake = Faker('ro_RO')#Generam nume romanesti

SPECIALTY_NAMES = [
    "Cardiologie", "Neurologie", "Pediatrie", "Ortopedie", "Dermatologie",
    "Oncologie", "Gastroenterologie", "Psihiatrie", "Oftalmologie", "Urologie",
    "Chirurgie Generala", "Endocrinologie", "ORL", "Reumatologie", "Nefrologie"
]


used_emails = set()

#Generator de email-uri unice
def get_unique_email(base_name, domain="spital.ro", is_doctor=True):
    if is_doctor:
        #base_name e de forma "Dr. Prenume Nume"
        #Extragem Nume si Prenume
        parts = base_name.split(' ')
        #parts[0] = Dr., parts[1] = Prenume, parts[2] = Nume
        #Vrem: nume.prenume@spital.ro
        local_part = f"{parts[2].lower()}.{parts[1].lower()}"
        email = f"{local_part}@{domain}"
        counter = 1
        #Daca exista deja, adaugam un numar: nume.prenume2@...
        original_local = local_part
        while email in used_emails:
            counter += 1
            email = f"{original_local}{counter}@{domain}"
    else:
        #Pentru pacienti nu trebuie ca email-ul sa fie corelat cu numele
        email = fake.email()
        while email in used_emails:
            email = fake.email()
    
    used_emails.add(email)
    return email


def insert_into_mongo(url, label, docs_data, pats_data):
    client = None
    try:
        print(f"--- Populare: {label} ---")
        #Conectare
        client = MongoClient(url, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') #Test conexiune
        
        db = client.db_an3#Creeaza baza daca nu exista
        
        #Doctori (creeaza baza daca nu exista)
        db.doctors.delete_many({}) #Curatam inainte
        if docs_data:
            db.doctors.insert_many(docs_data)
        print(f"    [OK] Inserat {len(docs_data)} doctori.")

        #Pacienti (Batch Insert, pentru ca sunt multi si cu date mari)
        db.patients.delete_many({}) #Curatam inainte
        batch_size = 500
        total_pats = 0
        
        if pats_data:
            for i in range(0, len(pats_data), batch_size):
                batch = pats_data[i:i + batch_size]
                db.patients.insert_many(batch)
                total_pats += len(batch)
            
        print(f"    [OK] Inserat {total_pats} pacienti.")
        print(f"--- FINALIZAT {label} ---\n")

    except ConnectionFailure:
        print(f"!!! EROARE: Nu ma pot conecta la {label} pe {url}")
        print("    Verifica daca containerul ruleaza si portul este corect.\n")
    except Exception as e:
        print(f"!!! EROARE GENERALA la {label}: {e}\n")
    finally:
        if client: client.close()



###PERSISTENTA#######
# Numele fisierelor unde salvam listele
FILES = {
    "oracle_specs": "data_set/data_oracle_specs.txt",
    "oracle_docs":  "data_set/data_oracle_docs.txt",
    "oracle_has":   "data_set/data_oracle_has.txt",
    "oracle_pats":  "data_set/data_oracle_pats.txt",
    "oracle_treats":"data_set/data_oracle_treats.txt",
    "mongo_docs":   "data_set/data_mongo_doctors.txt",
    "mongo_pats":   "data_set/data_mongo_patients.txt"
}

def save_list_to_txt(data_list, filename):
    """Scrie lista ca string in fisier text."""
    with open(filename, 'w', encoding='utf-8') as f:
        # str(data_list) transforma toata lista intr-un text imens
        f.write(str(data_list))
    print(f"   [SAVED] {filename}")

def load_list_from_txt(filename):
    """Citeste textul si il transforma inapoi in lista Python."""
    with open(filename, 'r', encoding='utf-8') as f:
        # eval() executa textul ca si cum ar fi cod Python.
        # Deoarece textul contine "datetime.datetime(...)", va recrea obiectele corect.
        content = f.read()
        return eval(content)
##########################################



def generate_data():
    #Verificam daca TOATE fisierele exista
    all_exist = all(os.path.exists(f) for f in FILES.values())

    data_specs = []
    data_docs = []
    data_has = []
    data_pats = []
    data_treats = []
    sample_doctors = []
    sample_patients = []


    if all_exist:
        print(">> [CACHE] Fisiere .txt gasite! Incarc datele de pe disc...")
        try:
            data_specs = load_list_from_txt(FILES["oracle_specs"])
            data_docs =  load_list_from_txt(FILES["oracle_docs"])
            data_has =  load_list_from_txt(FILES["oracle_has"])
            data_pats = load_list_from_txt(FILES["oracle_pats"])
            data_treats = load_list_from_txt(FILES["oracle_treats"])
            sample_doctors = load_list_from_txt(FILES["mongo_docs"])
            sample_patients = load_list_from_txt(FILES["mongo_pats"])


            print("ORACLE")
            conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DNS)
            cursor = conn.cursor()
            print(">> Conexiune reusita!")
            print(">> Curatare tabele (DELETE)...")
            #Ordinea conteaza din cauza Foreign Keys!
            cursor.execute("DELETE FROM Treatment")
            cursor.execute("DELETE FROM Has")
            cursor.execute("DELETE FROM Patient")
            cursor.execute("DELETE FROM Doctor")
            cursor.execute("DELETE FROM Specialty")

            #INSERARE IN BAZA
            print(f">> Inserare {len(data_specs)} specializari...")
            cursor.executemany("INSERT INTO Specialty VALUES (:1, :2)", data_specs)
            
            print(f">> Inserare {len(data_docs)} doctori...")
            cursor.executemany("INSERT INTO Doctor VALUES (:1, :2, :3, :4)", data_docs)
            
            print(f">> Inserare {len(data_has)} asocieri doctor-specialitate...")
            cursor.executemany("INSERT INTO Has VALUES (:1, :2)", data_has)
            
            print(f">> Inserare {len(data_pats)} pacienti...")
            cursor.executemany("INSERT INTO Patient VALUES (:1, :2, :3, :4)", data_pats)
            
            print(f">> Inserare {len(data_treats)} tratamente...")
            cursor.executemany("INSERT INTO Treatment VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", data_treats)

            conn.commit()
            print(">> POPULARE COMPLETA CU SUCCES PENTRU ORACLE!")
            print()
            print()
            print()


            print("MONGO")
            #Standalone
            insert_into_mongo(CONNECTION_STRING1, "STANDALONE (Port 27099)", sample_doctors, sample_patients)

            #Replica Set
            insert_into_mongo(CONNECTION_STRING2, "REPLICA SET (Port 27017-27018-27019)", sample_doctors, sample_patients)

            #Sharded Cluster
            insert_into_mongo(CONNECTION_STRING3, "SHARDED CLUSTER (Port 27067)", sample_doctors, sample_patients)

            return

        except Exception as e:
            print(f"!! Eroare la citirea fisierelor (probabil corupte): {e}")
            print("!! Voi regenera datele.")
            # Daca e eroare, continuam spre generare (nu dam return)

    
    #Daca nu exista, generam datele
    print(">> [GENERARE] Generez date noi cu Faker...")


    data_specs = []
    data_docs = []
    data_has = []
    data_pats = []
    data_treats = []
    sample_doctors = []
    sample_patients = []

    conn = None
    try:
        print(f">> Conectare la baza de date {DB_DNS}...")
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DNS)
        cursor = conn.cursor()
        print(">> Conexiune reusita!")


        print(">> Generare date in memorie...")

        #SPECIALIZARI 
        for i, name in enumerate(SPECIALTY_NAMES, 1):#incepem de la 1 cu indexul
            data_specs.append((i, name))

        #DOCTORI si RELATIA HAS
        for i in range(1, NUM_DOCTORS + 1):
            doc_id = 100 + i#luam id-uri de la 101 in sus
            #Nume realist: Dr. + Nume
            name = f"Dr. {fake.first_name()} {fake.last_name()}"
            #Email derivat din nume
            email = get_unique_email(name)
            hire_date = fake.date_between(start_date='-10y', end_date='-3y')
            data_docs.append((doc_id, name, hire_date, email))

            #Alocam 1 sau 2 specializari random acestui doctor
            #Selectam ID-uri din lista de specializari (1...len(SPECIALTY_NAMES))
            my_specs_ids = random.sample(range(1, len(SPECIALTY_NAMES) + 1), random.randint(1, 2))
            
            for spec_id in my_specs_ids:
                data_has.append((doc_id, spec_id))

        #Creem o mapa rapida {doctor_id: [lista_specializari_id]}
        #Ne ajuta sa generam tratamente valide (un cardiolog nu opereaza pe creier)
        doc_skills = {}
        for d_id, s_id in data_has:
            if d_id not in doc_skills: doc_skills[d_id] = []
            doc_skills[d_id].append(s_id)
        valid_doc_ids = list(doc_skills.keys())

        #PACIENTI si TRATAMENTE
        treat_id_counter = 90000 #Pornim ID-urile de la un numar mare

        pat_treatments = {}
        for i in range(1, NUM_PATIENTS + 1):
            pat_id = 5000 + i
            name = f"{fake.first_name()} {fake.last_name()}"
            email = get_unique_email(name, domain="gmail.com" if (pat_id%4) else "yahoo.com",is_doctor=False)
            born_date = fake.date_of_birth(minimum_age=2, maximum_age=90)
            
            data_pats.append((pat_id, name, born_date, email))

            # Generam un istoric medical (0 - MAX tratamente)
            num_treats = random.randint(0, MAX_TREATMENTS_PER_PATIENT)
            
            pat_treatments[pat_id]=[]
            for _ in range(num_treats):
                treat_id_counter += 1
                
                #Alegem un doctor random
                doc_id = random.choice(valid_doc_ids)
                #Alegem o specializare pe care acel doctor chiar o are
                spec_id = random.choice(doc_skills[doc_id])
                
                start = fake.date_between(start_date='-2y', end_date='today')
                #Data finala e dupa data de start
                end = (start + timedelta(days=random.randint(1, 25))) if (treat_id_counter%11) else None 
                
                diag = fake.sentence(nb_words=3).replace('.', '')
                med = f"{fake.word().capitalize()} {random.randint(10, 500)}mg"

                data_treats.append((
                    treat_id_counter, doc_id, pat_id, spec_id, 
                    start, end, diag, med
                ))
                pat_treatments[pat_id].append(treat_id_counter)


        print(">> Curatare tabele (DELETE)...")
        #Ordinea conteaza din cauza Foreign Keys!
        cursor.execute("DELETE FROM Treatment")
        cursor.execute("DELETE FROM Has")
        cursor.execute("DELETE FROM Patient")
        cursor.execute("DELETE FROM Doctor")
        cursor.execute("DELETE FROM Specialty")

        #INSERARE IN BAZA
        print(f">> Inserare {len(data_specs)} specializari...")
        cursor.executemany("INSERT INTO Specialty VALUES (:1, :2)", data_specs)
        save_list_to_txt(data_specs, FILES["oracle_specs"])
        
        print(f">> Inserare {len(data_docs)} doctori...")
        cursor.executemany("INSERT INTO Doctor VALUES (:1, :2, :3, :4)", data_docs)
        save_list_to_txt(data_docs, FILES["oracle_docs"])
        
        print(f">> Inserare {len(data_has)} asocieri doctor-specialitate...")
        cursor.executemany("INSERT INTO Has VALUES (:1, :2)", data_has)
        save_list_to_txt(data_has, FILES["oracle_has"])
        
        print(f">> Inserare {len(data_pats)} pacienti...")
        cursor.executemany("INSERT INTO Patient VALUES (:1, :2, :3, :4)", data_pats)
        save_list_to_txt(data_pats, FILES["oracle_pats"])
        
        print(f">> Inserare {len(data_treats)} tratamente...")
        cursor.executemany("INSERT INTO Treatment VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", data_treats)
        save_list_to_txt(data_treats, FILES["oracle_treats"])

        conn.commit()
        print(">> POPULARE COMPLETA CU SUCCES PENTRU ORACLE!")
        print()
        print()
        print()

    except oracledb.Error as e:
        print(f"!! Eroare Oracle: {e}")
        print()
        print()
        print()
    finally:
        if conn: conn.close()


    ###MONGODB###
    for doc in data_docs:
        sample_specialties = []
        for specialty_id in doc_skills[doc[0]]:
                sample_specialties.append({
                    'specialty_id': specialty_id,
                    'name': data_specs[specialty_id-1][1]
                })

        sample_doctors.append({
                '_id': doc[0],
                'name': doc[1],
                'hire_date': datetime.datetime.combine(doc[2], datetime.datetime.min.time()),##Mongo vrea si ora, nu doar ziua (setam ora la 00:00:00)
                'email': doc[3],
                'specialties': sample_specialties
        })

    for pat in data_pats:
        sample_treatments = []
        for treat_id in pat_treatments[pat[0]]:
                treat = data_treats[treat_id-90001]
                sample_treatments.append({
                    'treatment_id': treat[0],
                    'doctor_id': treat[1],
                    'specialty_id': treat[3],
                    'start_date': datetime.datetime.combine(treat[4], datetime.datetime.min.time()),
                    'end_date': datetime.datetime.combine(treat[5], datetime.datetime.min.time()) if treat[5] else None,
                    'diagnosis': treat[6],
                    'medication': treat[7],
                    'doctor_name': data_docs[treat[1]-101][1],
                    'specialty_name': data_specs[treat[3]-1][1]
                })

        sample_patients.append({
                '_id': pat[0],
                'name': pat[1],
                'born_date': datetime.datetime.combine(pat[2], datetime.datetime.min.time()),
                'email': pat[3],
                'treatments': sample_treatments
        })


    #Standalone
    insert_into_mongo(CONNECTION_STRING1, "STANDALONE (Port 27099)", sample_doctors, sample_patients)

    #Replica Set
    insert_into_mongo(CONNECTION_STRING2, "REPLICA SET (Port 27017-27018-27019)", sample_doctors, sample_patients)

    #Sharded Cluster
    insert_into_mongo(CONNECTION_STRING3, "SHARDED CLUSTER (Port 27067)", sample_doctors, sample_patients)

    save_list_to_txt(sample_doctors, FILES["mongo_docs"])
    save_list_to_txt(sample_patients, FILES["mongo_pats"])


if __name__ == "__main__":
    generate_data()