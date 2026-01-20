
--Curatarea schemei:
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE Treatment CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE Has CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE Patient CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE Doctor CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE Specialty CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL; END;
/


--Crearea tabelelor independente:
CREATE TABLE SPECIALTY (
    specialty_id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL
);

CREATE TABLE DOCTOR (
    doctor_id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    hire_date DATE,
    email VARCHAR2(100) UNIQUE
);

CREATE TABLE PATIENT (
    patient_id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    born_date DATE,
    email VARCHAR2(100) UNIQUE
);


--Crearea tabelelor asociative si dependente:
CREATE TABLE Has (
    doctor_id NUMBER,
    specialty_id NUMBER,
    CONSTRAINT pk_has PRIMARY KEY (doctor_id, specialty_id),
    CONSTRAINT fk_has_doctor FOREIGN KEY (doctor_id) REFERENCES DOCTOR(doctor_id),
    CONSTRAINT fk_has_specialty FOREIGN KEY (specialty_id) REFERENCES SPECIALTY(specialty_id)
);

CREATE TABLE Treatment (
    treatment_id NUMBER PRIMARY KEY,
    doctor_id NUMBER NOT NULL,
    patient_id NUMBER NOT NULL,
    specialty_id NUMBER NOT NULL,
    start_date DATE DEFAULT SYSDATE,
    end_date DATE,
    diagnosis VARCHAR2(255),
    medication VARCHAR2(255),
    CONSTRAINT fk_treat_doctor FOREIGN KEY (doctor_id) REFERENCES DOCTOR(doctor_id),
    CONSTRAINT fk_treat_patient FOREIGN KEY (patient_id) REFERENCES PATIENT(patient_id),
    CONSTRAINT fk_treat_specialty FOREIGN KEY (specialty_id) REFERENCES SPECIALTY(specialty_id)
);