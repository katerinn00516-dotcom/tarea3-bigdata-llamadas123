import json
import random
import time
from kafka import KafkaProducer

TOPIC = "llamadas_123"

# (tipo, peso, probabilidad de quedar sin traslado)
TIPOS = [("HERIDO", 1419, 0.115), ("ENFERMO", 742, 0.376), ("EVERES", 681, 0.229),
         ("TRASTMENT", 619, 0.084), ("INCONSCIEN", 442, 0.391), ("CONVULSION", 400, 0.193),
         ("DOLTOR", 305, 0.246), ("INTSUI", 294, 0.105), ("SINTOGASTR", 262, 0.344),
         ("ACV", 234, 0.201), ("AMESUI", 184, 0.092), ("PATGIN", 136, 0.154),
         ("CAIALT", 133, 0.023)]
LOCALIDADES = [("KENNEDY", 809), ("ENGATIVA", 594), ("SUBA", 545), ("BOSA", 501),
               ("PUENTE ARANDA", 492), ("CIUDAD BOLIVAR", 441), ("RAFAEL URIBE URIBE", 321),
               ("SAN CRISTOBAL", 312), ("FONTIBON", 300), ("USME", 263), ("USAQUEN", 217),
               ("LOS MARTIRES", 212), ("SANTA FE", 196), ("TEUSAQUILLO", 178),
               ("BARRIOS UNIDOS", 167), ("TUNJUELITO", 165), ("CHAPINERO", 160),
               ("ANTONIO NARINO", 128), ("LA CANDELARIA", 25)]
PRIORIDADES = [("ALTA", 3202), ("CRITICA", 1581), ("MEDIA", 872), ("BAJA", 201)]


def generar_evento():
    tipo, _, p = random.choices(TIPOS, weights=[t[1] for t in TIPOS])[0]
    return {
        "incidente_id": random.randint(100000, 999999),
        "localidad": random.choices([l[0] for l in LOCALIDADES], weights=[l[1] for l in LOCALIDADES])[0],
        "tipo_incidente": tipo,
        "prioridad": random.choices([x[0] for x in PRIORIDADES], weights=[x[1] for x in PRIORIDADES])[0],
        "edad": random.randint(0, 95),
        "genero": random.choice(["FEMENINO", "MASCULINO"]),
        "sin_recepcion": 1 if random.random() < p else 0,
        "timestamp": int(time.time()),
    }


producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                         value_serializer=lambda x: json.dumps(x).encode("utf-8"))

while True:
    evento = generar_evento()
    producer.send(TOPIC, value=evento)
    print("Sent:", evento)
    time.sleep(0.5)
