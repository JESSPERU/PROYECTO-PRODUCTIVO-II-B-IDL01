import pandas as pd
import numpy as np
from supabase import create_client, Client

# --- CONFIGURACIÓN DE CREDENCIALES ---
SUPABASE_URL = "https://shyzhukbqguogrryxzsm.supabase.co"  # URL original de tus credenciales
SUPABASE_KEY = "sb_publishable_Ugj22lp8MbdT00sPqECoDg_B5Wzlsd7" # KEY original de tus credenciales

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- FUNCIONES BASE ---
def extraer_bronze(tabla):
    # Extrae todos los registros de la tabla indicada en el esquema 'bronze'
    res = supabase.schema('bronze').table(tabla).select("*").execute()
    return pd.DataFrame(res.data)

def cargar_silver(df, tabla):
    # Convertir NaN a None para que Supabase lo acepte como NULL
    df = df.replace({np.nan: None})
    datos = df.to_dict(orient='records')
    batch_size = 500
    for i in range(0, len(datos), batch_size):
        try:
            supabase.schema('silver').table(tabla).insert(datos[i:i+batch_size]).execute()
        except Exception as e:
            print(f"❌ Error al cargar lote en silver.{tabla}: {e}")
            
    print(f"✅ silver.{tabla}: {len(datos)} registros guardados.")

def limpiar_fechas(serie):
    return pd.to_datetime(serie, format='mixed', dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

def procesar_limpieza():
    print("⏳ 1. Extrayendo 9 tablas de Bronce...")
    dfs = {
        'clientes': extraer_bronze('sap_clientes'),
        'productos': extraer_bronze('sap_productos'),
        'canales': extraer_bronze('sap_canales'),
        'campanas': extraer_bronze('ads_campanas'),
        'v_cabecera': extraer_bronze('sap_ventas_cabecera'),
        'v_detalle': extraer_bronze('sap_ventas_detalle'),
        'inventario': extraer_bronze('sap_inventario_diario'),
        'insights': extraer_bronze('ads_insights_diario'),
        'clima': extraer_bronze('clima_diario_log')
    }

    print("🧹 2. Limpiando, Tipando y ELIMINANDO DUPLICADOS...")

    # --- ELIMINACIÓN DE DUPLICADOS EN LLAVES PRIMARIAS ---
    dfs['clientes'] = dfs['clientes'].drop_duplicates(subset=['id_cliente'], keep='first')
    dfs['productos'] = dfs['productos'].drop_duplicates(subset=['id_sku'], keep='first')
    dfs['canales'] = dfs['canales'].drop_duplicates(subset=['id_canal'], keep='first')
    dfs['campanas'] = dfs['campanas'].drop_duplicates(subset=['id_campana'], keep='first')
    dfs['v_cabecera'] = dfs['v_cabecera'].drop_duplicates(subset=['id_transaccion'], keep='first')

    # Maestros
    dfs['clientes']['nombre_empresa'] = dfs['clientes']['nombre_empresa'].str.strip()
    dfs['productos']['lead_time_dias'] = pd.to_numeric(dfs['productos']['lead_time_dias'], errors='coerce')
    dfs['canales']['comision_porcentaje'] = pd.to_numeric(dfs['canales']['comision_porcentaje'], errors='coerce')
    dfs['campanas']['fecha_inicio'] = limpiar_fechas(dfs['campanas']['fecha_inicio'])
    dfs['campanas']['fecha_fin'] = limpiar_fechas(dfs['campanas']['fecha_fin'])

    # Ventas (Eliminar "Fecha_Error")
    dfs['v_cabecera']['fecha_venta'] = limpiar_fechas(dfs['v_cabecera']['fecha_venta'])
    dfs['v_cabecera'] = dfs['v_cabecera'].dropna(subset=['fecha_venta'])
    dfs['v_detalle']['cantidad_vendida'] = pd.to_numeric(dfs['v_detalle']['cantidad_vendida'], errors='coerce')
    dfs['v_detalle']['precio_unitario_aplicado'] = pd.to_numeric(dfs['v_detalle']['precio_unitario_aplicado'], errors='coerce')

    # Inventario
    dfs['inventario']['fecha_foto'] = limpiar_fechas(dfs['inventario']['fecha_foto'])
    dfs['inventario']['stock_disponible_cierre'] = pd.to_numeric(dfs['inventario']['stock_disponible_cierre'], errors='coerce')
    dfs['inventario'] = dfs['inventario'].dropna(subset=['fecha_foto'])

    # Publicidad (Imputar nulos con 0.0)
    dfs['insights']['fecha_metrica'] = limpiar_fechas(dfs['insights']['fecha_metrica'])
    dfs['insights']['inversion_usd'] = pd.to_numeric(dfs['insights']['inversion_usd'], errors='coerce').fillna(0.0)
    dfs['insights']['clics'] = pd.to_numeric(dfs['insights']['clics'], errors='coerce').fillna(0)
    dfs['insights']['impresiones'] = pd.to_numeric(dfs['insights']['impresiones'], errors='coerce').fillna(0)
    dfs['insights'] = dfs['insights'].dropna(subset=['fecha_metrica'])

    # Clima
    dfs['clima']['fecha_medicion'] = limpiar_fechas(dfs['clima']['fecha_medicion'])
    dfs['clima']['temperatura_promedio_celsius'] = pd.to_numeric(dfs['clima']['temperatura_promedio_celsius'], errors='coerce')
    dfs['clima'] = dfs['clima'].dropna(subset=['fecha_medicion'])

    print("🚀 3. Cargando a la Capa Silver...")
    tablas_destino = [
        'sap_clientes', 'sap_productos', 'sap_canales', 'ads_campanas', 
        'sap_ventas_cabecera', 'sap_ventas_detalle', 'sap_inventario_diario', 
        'ads_insights_diario', 'clima_diario_log'
    ]
    
    for key, tabla in zip(dfs.keys(), tablas_destino):
        print(f"Subiendo datos limpios a silver.{tabla}...")
        cargar_silver(dfs[key], tabla)
        
    print("🎉 Proceso de limpieza finalizado.")

if __name__ == "__main__":
    procesar_limpieza()