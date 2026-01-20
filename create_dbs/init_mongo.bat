@echo off

:: REPLICA SET SIMPLU 
echo.
echo [1/8] Configurare Replica Set Simplu (RS0)...
docker exec mongo_rs1 mongosh --port 27017 --eval "try { rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'mongo_rs1:27017'}, {_id: 1, host: 'mongo_rs2:27017'}, {_id: 2, host: 'mongo_rs3:27017'}]}) } catch(e) { print('RS0 deja initiat sau eroare: ' + e) }"


:: CLUSTER SHARDED (INFRASTRUCTURA)
echo.
echo [2/8] Configurare Config Servers (CFGRS)...
docker exec mongo_cfg1 mongosh --port 27017 --eval "try { rs.initiate({_id: 'cfgrs', configsvr: true, members: [{_id: 0, host: 'mongo_cfg1:27017'}, {_id: 1, host: 'mongo_cfg2:27017'}]}) } catch(e) { print('CFG deja initiat sau eroare: ' + e) }"

echo.
echo [3/8] Configurare Shard 1 (SHARD1RS)...
docker exec mongo_shard11 mongosh --port 27017 --eval "try { rs.initiate({_id: 'shard1rs', members: [{_id: 0, host: 'mongo_shard11:27017'}, {_id: 1, host: 'mongo_shard12:27017'}, {_id: 2, host: 'mongo_shard13:27017'}]}) } catch(e) { print('Shard1 deja initiat sau eroare: ' + e) }"

echo.
echo [4/8] Configurare Shard 2 (SHARD2RS)...
docker exec mongo_shard21 mongosh --port 27017 --eval "try { rs.initiate({_id: 'shard2rs', members: [{_id: 0, host: 'mongo_shard21:27017'}, {_id: 1, host: 'mongo_shard22:27017'}, {_id: 2, host: 'mongo_shard23:27017'}]}) } catch(e) { print('Shard2 deja initiat sau eroare: ' + e) }"
echo.
echo [5/8] Asteptam Router-ul sa fie gata (Polling)...
:wait_mongos
docker exec mongo_router mongosh --quiet --eval "sh.status().ok" >nul 2>&1
if errorlevel 1 (
    echo    ... Router inca porneste. Astept 3 secunde ...
    timeout /t 3 >nul
    goto wait_mongos
)
echo    [OK] Router Online.


:: LEGAREA PENTRU CLUSTER SHARDED
echo.
echo [6/8] Conectare Shard-uri la Router...
docker exec mongo_router mongosh --port 27017 --eval "try { sh.addShard('shard1rs/mongo_shard11:27017,mongo_shard12:27017,mongo_shard13:27017'); sh.addShard('shard2rs/mongo_shard21:27017,mongo_shard22:27017,mongo_shard23:27017'); } catch(e) { print('Shard-uri deja adaugate sau eroare: ' + e) }"

echo.
echo [7/8] Pauza scurta (5 sec) pentru propagarea config-ului...
timeout /t 5 /nobreak

:: ACTIVAREA SHARDING-ULUI - VERSIUNEA CORECTATA
echo.
echo [8/8] Definire reguli Sharding (Intr-o singura comanda)...
:: AICI ESTE SCHIMBAREA CHEIE: Totul intr-un singur EVAL si setare manuala ChunkSize
docker exec mongo_router mongosh --port 27017 --eval "var configDB = db.getSiblingDB('config'); configDB.settings.updateOne({_id: 'chunksize'}, {$set: {value: 1}}, {upsert: true}); print('Chunk Size setat la 1MB'); sh.enableSharding('db_an3'); db.getSiblingDB('db_an3').patients.createIndex({_id: 'hashed'}); sh.shardCollection('db_an3.patients', {_id: 'hashed'}); print('Reguli aplicate!');"

echo.
echo MAI ASTEPTAM PUTIN PENTRU NOILE REGULI
timeout /t 5 /nobreak

echo.
echo Verificam configuratia finala:
docker exec mongo_router mongosh --quiet --eval "print('ChunkSize:'); printjson(db.getSiblingDB('config').settings.findOne({_id:'chunksize'})); print('Colectie Config:'); printjson(db.getSiblingDB('config').collections.findOne({_id:'db_an3.patients'}));"

echo.
echo [DONE] Clusterul este configurat!
pause