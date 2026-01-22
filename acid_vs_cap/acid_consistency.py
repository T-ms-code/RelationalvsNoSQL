from pymongo import MongoClient
import oracledb
import datetime


DB_USER = "db_an3"
DB_PASS = "parola123"
DB_DNS  = "localhost:1521/mihaibase"
CONNECTION_STRING = "mongodb://localhost:27099/"


def fct_for_oracle():
    print("\n=========== TEST CONSISTENTA ORACLE ===========")

    try:
        conn = oracledb.connect(
            user=DB_USER,
            password=DB_PASS,
            dsn=DB_DNS
        )
        cursor = conn.cursor()
        print("[OK] Conectare Oracle")
    except Exception as e:
        print("[ERROR] Conectare Oracle:", e)
        return
    

    #Inserare cu cheie primara duplicata
    try:
        cursor.execute(
            "INSERT INTO Doctor VALUES (:1, :2, :3, :4)",
            (101, "Duplicat", datetime.datetime.now(), "dup@gmail.com")
        )
        conn.commit()
        print("Cheie primara duplicata acceptata")
    except Exception as e:
        print("Cheie primara duplicata respinsa:", e)

    #nserare cu campuri lipsa
    try:
        cursor.execute(
            "INSERT INTO Doctor (doctor_id, name) VALUES (:1, :2)",
            (10000, "Incomplet")
        )
        conn.commit()
        print("Lista de campuri incompleta acceptata")
    except Exception as e:
        print("Lista de campuri incompleta respinsa:", e)

    #Inserare null peste not null
    try:
        cursor.execute(
            "INSERT INTO Doctor (doctor_id, name) VALUES (:1, :2)",
            (10001, None)
        )
        conn.commit()
        print("Nume null acceptat")
    except Exception as e:
        print("Nume null respins:", e)

    #Inserare email nonunique
    try:
        cursor.execute(
            "INSERT INTO Doctor VALUES (:1, :2, :3, :4)",
            (10002, "X", datetime.datetime.now(), "x@gmail.com")
        )
        cursor.execute(
            "INSERT INTO Doctor VALUES (:1, :2, :3, :4)",
            (10003, "Y", datetime.datetime.now(), "x@gmail.com")
        )
        conn.commit()
        print("Inserare email nonunique acceptata")
    except Exception as e:
        print("Inserare email nonunique respinsa:", e)       
      
    #Inserare cheie externa nonexistena
    try:
        cursor.execute(
            "INSERT INTO Treatment VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
            (1, 101, 5001, 70, datetime.datetime.now(), datetime.datetime.now(), "x", "y")
        )
        conn.commit()
        print("Inserare cheie externa (specialty_id) nonexistena acceptata")
    except Exception as e:
        print("Inserare cheie externa (specialty_id) nonexistena respinsa:", e) 
    conn.close()



def fct_for_mongo():
    print("\n=========== TEST CONSISTENTA MONGODB ===========")

    client = MongoClient(CONNECTION_STRING)
    db = client.db_an3
    coll = db.doctors

    #Inserare cu _id duplicat
    try:
        coll.insert_one({
            "_id": 101,
            "name": "Duplicat",
        })
        print(" _id duplicat acceptat")
    except Exception as e:
        print("_id duplicat respins:", e)

    #Inserare cu campuri lipsa / schema diferita
    try:
        coll.insert_one({
            "_id": 10000,
            "name": "Incomplet",
            "orice": "vreau"
        })
        print("Document cu schema diferita acceptat")
    except Exception as e:
        print("Document cu schema diferita respins:", e)

    #Inserare null peste not null
    try:
        coll.insert_one({
            "_id": 10001,
            "name": None,
            "orice": "vreau"
        })
        print("Nume null acceptat")
    except Exception as e:
        print("Nume null respins:", e)


    #Inserare email nonunique
    try:
        coll.insert_one({
            "_id": 10002,
            "name": "X",
            "hire_date": datetime.datetime.now(),
            "email": "x@gmail.com",
            "specialties": []
        })
        coll.insert_one({
            "_id": 10003,
            "name": "Y",
            "hire_date": datetime.datetime.now(),
            "email": "x@gmail.com",
            "specialties": []
        })
        print("Inserare email nonunique acceptata")
    except Exception as e:
        print("Inserare email nonunique respinsa:", e)

    
    #Inserare cheie externa nonexistena
    coll = db.patients
    try:
        coll.insert_one({
            "_id": 4002,
            "name": "X",
            "born_date": datetime.datetime.now(),
            "email": "x@gmail.com",
            "treatments": [{
                "treatment_id":1,
                "doctor_id": 101,
                "specialty_id": 70,
                "start_date": datetime.datetime.now(),
                "end_date":datetime.datetime.now(),
                "diagnosis": "a",
                "medication":"b",
                "doctor_name":"c",
                "specialty_name":"d"
            }]
        })
        print("Inserare cheie externa (specialty_id) nonexistena acceptata")
    except Exception as e:
        print("Inserare cheie externa (specialty_id) nonexistena respinsa:", e)

    client.close()



if __name__ == "__main__":
    fct_for_oracle()
    fct_for_mongo()
