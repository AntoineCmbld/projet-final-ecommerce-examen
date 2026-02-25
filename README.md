# Projet Final (Examen) — Sans démo live

Ce dossier est prêt à distribuer. L'évaluation se fait sur la **reproductibilité** (commandes, logs, captures) et le rapport.

## 1) Démarrer l’infrastructure
```bash
docker-compose up -d
```

Services disponibles :
- Kafka : `localhost:9092`
- Flink UI : http://localhost:8081

## 2) Installer les dépendances Python (machine hôte)
```bash
pip install -r requirements.txt
```

## 3) Produire des événements (machine hôte)
Dans un terminal A :
```bash
python scripts/producer.py
```
➡️ Laissez ce script tourner pendant toute la session.

## 4) Lancer le job Flink (dans le conteneur)
Dans un terminal B, exécutez le job :
```bash
docker exec -it flink-jobmanager bash -lc "flink run -py /opt/flink/usrlib/pipeline.py"
```

## 5) Simuler une anomalie (optionnel, pour valider)
Dans un terminal C :
```bash
python scripts/anomaly_simulator.py
```

## 6) Quoi vérifier ?
- Dans les logs du job Flink : présence d’un message d’anomalie (ex. `[ANOMALY] user_id=...`)
- Dans l’UI Flink : job `RUNNING` et métriques de throughput

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh
  --bootstrap-server localhost:9092
  --topic ecommerce-anomalies
  --from-beginning
```

## 7) Livrables attendus
- Code + README + rapport PDF (10 pages max)
- Preuves d’exécution : logs/captures
