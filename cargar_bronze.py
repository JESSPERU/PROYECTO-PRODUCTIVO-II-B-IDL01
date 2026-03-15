import pandas as pd
import numpy as np
import os
from supabase import create_client, Client

# --- CONFIGURACIÓN DE CREDENCIALES ---
SUPABASE_URL = "https://shyzhukbqguogrryxzsm.supabase.co"
SUPABASE_KEY = "sb_publishable_Ugj22lp8MbdT00sPqECoDg_B5Wzlsd7"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- DICCIONARIO DE ARCHIVOS VS TABLAS ---
# Relacionamos el nombre exacto de tu CSV con el nombre de tu tabla en Supabase
archivos_config = {
    'sap_clientes_maestro.csv': 'sap_clientes',
    'sap_productos_maestro.csv': 'sap_productos',
    'sap_canales_maestro.csv': 'sap_canales',
    'ads_campanas_maestro.csv': 'ads_campanas',
    'sap_ventas_cabecera.csv': 'sap_ventas_cabecera',
    'sap_ventas_detalle.csv': 'sap_ventas_detalle',
    'sap_inventario_diario.csv': 'sap_inventario_diario',
    'ads_insights_diario.csv': 'ads_insights_diario',
    'clima_diario_log.csv': 'clima_diario_log'
}

def cargar_datos_a_bronze():
    ruta_base = '.' # Busca los archivos en la misma carpeta donde está este script

    for archivo_csv, nombre_tabla in archivos_config.items():
        ruta_completa = os.path.join(ruta_base, archivo_csv)
        
        # Verificar si el archivo CSV realmente está en la carpeta
        if not os.path.exists(ruta_completa):
            print(f"⚠️ Archivo no encontrado en la carpeta: {archivo_csv}")
            continue
            
        print(f"⏳ Cargando {archivo_csv} a la tabla bronze.{nombre_tabla}...")
        
        # Leer el CSV forzando todo a texto (str) para evitar problemas de tipos en Bronze
        df = pd.read_csv(ruta_completa, dtype=str)
        
        # Convertir valores nulos/vacíos a None para que Supabase los lea como NULL
        df = df.replace({np.nan: None})
        
        # Transformar a formato diccionario (JSON-like)
        datos = df.to_dict(orient='records')
        
        # Cargar a Supabase en bloques de 500 registros
        batch_size = 500
        for i in range(0, len(datos), batch_size):
            lote = datos[i:i + batch_size]
            try:
                # Se especifica el esquema 'bronze' tal como lo tienes en Supabase
                supabase.schema('bronze').table(nombre_tabla).insert(lote).execute()
            except Exception as e:
                print(f"❌ Error al cargar un lote en {nombre_tabla}: {e}")
        
        print(f"✅ Tabla {nombre_tabla} cargada con éxito ({len(datos)} registros).\n")

if __name__ == "__main__":
    print("🚀 Iniciando ingesta de datos a la Capa Bronze...")
    cargar_datos_a_bronze()
    print("🎉 Ingesta finalizada.")