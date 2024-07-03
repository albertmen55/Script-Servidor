from kafka import KafkaConsumer
import json
import os
import base64
from PIL import Image
import torch
from transformers import CLIPProcessor, CLIPModel
import re

# Definimos la lista de IDs de cajones
id_cajon = [5, 15, 16]

# Funcion que crea las carpetas y guarda los archivos en el directorio base especificado.
def create_directories_and_files(directories, base_path):
    for directory in directories:
        try:
            directory_path = os.path.join(base_path, os.path.basename(directory['directory']))
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)

            # Guardar archivos en la carpeta creada
            for file_name, file_data_base64 in directory['files'].items():
                file_path = os.path.join(directory_path, file_name)
                file_data = base64.b64decode(file_data_base64)
                with open(file_path, 'wb') as f:
                    f.write(file_data)

        except Exception as e:
            print(f"Error al crear la carpeta {directory['directory']}: {e}")

    # Buscar la fecha en el nombre de la carpeta
    folder_name = os.path.basename(directory['directory'])
    date_pattern = r"\d{4}_\d{2}_\d{2}_\d{2}:\d{2}:\d{2}"
    date_match = re.search(date_pattern, folder_name)
    if date_match:
        date_str = date_match.group()
    else:
        date_str = "Fecha desconocida"

    # Buscar el ID del cajon en el nombre de la carpeta
    cajon_pattern = re.compile(r"\[(\d+)\]")
    for match in cajon_pattern.findall(folder_name):
        if int(match) in id_cajon:
            print(f"PERTURBACIÓN EN CAJON {match} a las {date_str}")

    # Llamar a la funcion de analisis para archivos que empiezan con "recortada"
    for file_name in directory['files'].keys():
        if file_name.startswith("recortada"):
            section_pattern = re.compile(r"recortada_(\d+)_")
            section_match = section_pattern.search(file_name)
            if section_match:
                section_num = section_match.group(1)
                print(f"RESULTADO SECCIÓN {section_num}")
            analyze_image(os.path.join(directory_path, file_name))

# Funcion para analizar las imagenes usando CLIP
def analyze_image(image_path):

    #Definimos modelo y palabras o frases
    model_name = "openai/clip-vit-base-patch32"
    captions = [
        "empty drawer", "not empty drawer", "empty box", "not empty box",
        "drawer with items", "drawer without items", "box with items", "box without items",
        "empty container", "full container", "object inside", "no object inside"
    ]

    # Cargamos el modelo y el procesador
    model = CLIPModel.from_pretrained(model_name)
    processor = CLIPProcessor.from_pretrained(model_name)

    # Procesamos la imagen y los textos
    image = Image.open(image_path).convert("RGB")
    inputs = processor(text=captions, images=image, return_tensors="pt", padding=True)

    # Movemos el modelo y los tensores al dispositivo adecuado
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = model.to(device)
    inputs = {k: v.to(device) for k, v in inputs.items()}

    # Obtenemos las caracteristicas de la imagen y el texto
    with torch.no_grad():
        outputs = model(**inputs)
        logits_per_image = outputs.logits_per_image
        probs = logits_per_image.softmax(dim=1)

    # Determinamos si hay elementos dentro o no
    empty_probs = [
        probs[0][captions.index("empty drawer")].item(),
        probs[0][captions.index("empty box")].item(),
        probs[0][captions.index("drawer without items")].item(),
        probs[0][captions.index("box without items")].item(),
        probs[0][captions.index("empty container")].item(),
        probs[0][captions.index("no object inside")].item()
    ]

    not_empty_probs = [
        probs[0][captions.index("not empty drawer")].item(),
        probs[0][captions.index("not empty box")].item(),
        probs[0][captions.index("drawer with items")].item(),
        probs[0][captions.index("box with items")].item(),
        probs[0][captions.index("full container")].item(),
        probs[0][captions.index("object inside")].item()
    ]

    #Calculamos los promedios
    avg_empty_prob = sum(empty_probs) / len(empty_probs)
    avg_not_empty_prob = sum(not_empty_probs) / len(not_empty_probs)

    final_verdict = "Vacío" if avg_empty_prob > avg_not_empty_prob else "No Vacío"

    print(f"ESTADO ACTUAL ------> {final_verdict}")
    print("\n")


def main():
    #Definimos los parametros necesarios
    kafka_server = '169.254.52.160:9092'
    topic = 'topic_oficial_tfg'
    base_directory = './consumido'

    # Creamos el consumidor de Kafka
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[kafka_server],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Consumidor de Kafka creado correctamente")

    print("Iniciando la escucha de mensajes de Kafka...")

    #Vamos procesando cada mensaje
    for message in consumer:
        try:
            directories = [message.value]
            create_directories_and_files(directories, base_directory)
        except json.JSONDecodeError as e:
            print(f"Error al decodificar el mensaje: {e}")
        except Exception as e:
            print(f"Error procesando el mensaje: {e}")

    consumer.close()

if __name__ == "__main__":
    main()
