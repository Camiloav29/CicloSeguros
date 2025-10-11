import os
import json

# Define la ruta base del directorio de configuración
CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')

def get_list(list_name):
    """
    Carga y devuelve una lista desde un archivo JSON en el directorio de configuración.
    """
    # Asegurarse de que el directorio de configuración exista
    if not os.path.exists(CONFIG_DIR):
        os.makedirs(CONFIG_DIR)

    filepath = os.path.join(CONFIG_DIR, f"{list_name}.json")

    # Si el archivo no existe, crearlo con una lista vacía
    if not os.path.exists(filepath):
        save_list(list_name, [])
        return []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        # Si hay un error de decodificación o el archivo no se encuentra,
        # devolver una lista vacía para evitar que la aplicación falle.
        return []

def save_list(list_name, data):
    """
    Guarda una lista en un archivo JSON en el directorio de configuración.
    """
    # Asegurarse de que el directorio de configuración exista
    if not os.path.exists(CONFIG_DIR):
        os.makedirs(CONFIG_DIR)

    filepath = os.path.join(CONFIG_DIR, f"{list_name}.json")
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            # Usar indent=4 para que el JSON sea legible
            # ensure_ascii=False para guardar correctamente caracteres como tildes
            json.dump(data, f, indent=4, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error al guardar la lista '{list_name}': {e}")
        return False

def get_all_list_names():
    """
    Devuelve los nombres de todos los archivos JSON (listas) disponibles
    en el directorio de configuración.
    """
    if not os.path.exists(CONFIG_DIR):
        return []

    # Listar todos los archivos, filtrar por .json y quitar la extensión
    files = [f.replace('.json', '') for f in os.listdir(CONFIG_DIR) if f.endswith('.json')]
    return sorted(files)