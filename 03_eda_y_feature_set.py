import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client, Client

# --- CONFIGURACIÓN DE CREDENCIALES ---
SUPABASE_URL = "https://shyzhukbqguogrryxzsm.supabase.co"
SUPABASE_KEY = "sb_publishable_Ugj22lp8MbdT00sPqECoDg_B5Wzlsd7"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def extraer_silver(tabla):
    res = supabase.schema('silver').table(tabla).select("*").execute()
    return pd.DataFrame(res.data)

def generar_eda_y_feature_set():
    print("⏳ Extrayendo tablas de Silver...")
    dfs = {
        'v_cabecera' : extraer_silver('sap_ventas_cabecera'),
        'v_detalle'  : extraer_silver('sap_ventas_detalle'),
        'productos'  : extraer_silver('sap_productos'),
        'insights'   : extraer_silver('ads_insights_diario'),
        'clima'      : extraer_silver('clima_diario_log'),
        'canales'    : extraer_silver('sap_canales'),
    }

    print("✅ Silver cargado:")
    for k, v in dfs.items():
        print(f"   {k}: {v.shape}")
        
    print("📊 Construyendo Feature Set Día + SKU + Canal...")

    # 1. Unir detalle + cabecera (obtener fecha y canal) [cite: 8, 9]
    df_ventas = pd.merge(dfs['v_detalle'], dfs['v_cabecera'], on='id_transaccion', how='inner')

    # 2. Unir con productos (obtener descripcion, categoria, lead_time) [cite: 9]
    df_ventas = pd.merge(df_ventas, dfs['productos'], on='id_sku', how='left')

    # 3. Unir con canales (obtener nombre_canal) [cite: 9]
    df_ventas = pd.merge(df_ventas, dfs['canales'], on='id_canal', how='left')

    # 4. Agrupar por Día + SKU + Canal [cite: 9]
    # Aseguramos que las columnas numéricas sean float para poder sumarlas/promediarlas
    df_ventas['cantidad_vendida'] = pd.to_numeric(df_ventas['cantidad_vendida'], errors='coerce')
    df_ventas['precio_unitario_aplicado'] = pd.to_numeric(df_ventas['precio_unitario_aplicado'], errors='coerce')
    
    df_eda = df_ventas.groupby(
        ['fecha_venta', 'id_sku', 'descripcion', 'categoria', 'id_canal', 'lead_time_dias']
    ).agg(
        cantidad_vendida=('cantidad_vendida', 'sum'),
        precio_promedio=('precio_unitario_aplicado', 'mean')
    ).reset_index()

    # 5. Ads por día [cite: 9]
    dfs['insights']['inversion_usd'] = pd.to_numeric(dfs['insights']['inversion_usd'], errors='coerce')
    ads_diarios = dfs['insights'].groupby('fecha_metrica').agg(
        inversion_usd=('inversion_usd', 'sum')
    ).reset_index()

    # 6. Merge con Ads y Clima [cite: 9]
    df_eda = df_eda.merge(ads_diarios, left_on='fecha_venta', right_on='fecha_metrica', how='left')
    df_eda = df_eda.merge(dfs['clima'], left_on='fecha_venta', right_on='fecha_medicion', how='left')

    # 7. Limpiar nulos [cite: 9]
    df_eda['inversion_usd'] = df_eda['inversion_usd'].fillna(0)
    df_eda['temperatura_promedio_celsius'] = pd.to_numeric(df_eda['temperatura_promedio_celsius'], errors='coerce')
    df_eda['temperatura_promedio_celsius'] = df_eda['temperatura_promedio_celsius'].interpolate()
    df_eda['fecha_venta'] = pd.to_datetime(df_eda['fecha_venta'])

    print(f"✅ Feature Set listo: {df_eda.shape}")
    print(f"   Granularidad: {df_eda['fecha_venta'].nunique()} días | {df_eda['id_sku'].nunique()} SKUs | {df_eda['id_canal'].nunique()} canales")
    print(df_eda.head(5).to_string())
    
    print("📈 Iniciando EDA ")

    # ── 1. CORRELACIÓN GLOBAL ────────────────────────────────────── [cite: 10]
    cols = ['cantidad_vendida', 'inversion_usd', 'temperatura_promedio_celsius', 'precio_promedio']
    matriz_corr = df_eda[cols].corr()

    plt.figure(figsize=(8, 6))
    sns.heatmap(matriz_corr, annot=True, cmap='Pastel2', fmt=".2f",
                linewidths=0.5, linecolor='white', vmin=-1, vmax=1)
    plt.title("Correlación Global: Demanda vs Variables (Día + SKU + Canal)", fontsize=13, pad=12)
    plt.tight_layout()
    plt.show()

    # ── 2. CORRELACIÓN POR SKU ───────────────────────────────────── [cite: 10, 11]
    print("\n" + "="*50)
    print("CORRELACIONES POR SKU")
    print("="*50)

    skus = df_eda['id_sku'].unique()
    fig, axes = plt.subplots(1, len(skus), figsize=(6 * len(skus), 5))
    if len(skus) == 1: axes = [axes] # Manejo por si solo hay 1 SKU

    for ax, sku in zip(axes, skus):
        subset = df_eda[df_eda['id_sku'] == sku]
        desc = subset['descripcion'].iloc[0]
        corr = subset[cols].corr()
        corr_vs_demanda = corr['cantidad_vendida'].drop('cantidad_vendida')
        print(f"\n📦 {sku} — {desc} (n={len(subset)})")
        print(corr_vs_demanda.round(3).to_string())
        sns.heatmap(corr, annot=True, cmap='Pastel2', fmt=".2f",
                    linewidths=0.5, linecolor='white', vmin=-1, vmax=1, ax=ax)
        ax.set_title(f"{desc}", fontsize=11)

    plt.suptitle("Correlación de Pearson por SKU (Día + SKU + Canal)", fontsize=13, y=1.02)
    plt.tight_layout()
    plt.show()

    # ── 3. CORRELACIÓN POR CANAL ─────────────────────────────────── [cite: 11, 12]
    print("\n" + "="*50)
    print("CORRELACIONES POR CANAL")
    print("="*50)

    canales = df_eda['id_canal'].unique()
    fig, axes = plt.subplots(1, len(canales), figsize=(5 * len(canales), 5))
    if len(canales) == 1: axes = [axes]

    for ax, canal in zip(axes, canales):
        subset = df_eda[df_eda['id_canal'] == canal]
        corr = subset[cols].corr()
        corr_vs_demanda = corr['cantidad_vendida'].drop('cantidad_vendida')
        print(f"\n🏪 Canal: {canal} (n={len(subset)})")
        print(corr_vs_demanda.round(3).to_string())
        sns.heatmap(corr, annot=True, cmap='Pastel2', fmt=".2f",
                    linewidths=0.5, linecolor='white', vmin=-1, vmax=1, ax=ax)
        ax.set_title(f"Canal: {canal}", fontsize=11)

    plt.suptitle("Correlación de Pearson por Canal", fontsize=13, y=1.02)
    plt.tight_layout()
    plt.show()

    # ── 4. SCATTER: Publicidad vs Ventas por SKU ─────────────────── [cite: 12, 13]
    g = sns.FacetGrid(df_eda, col='descripcion', hue='id_canal',
                      height=4, aspect=1.2, palette='tab10')
    g.map(sns.scatterplot, 'inversion_usd', 'cantidad_vendida', alpha=0.7, edgecolor='white', linewidth=0.3)
    g.add_legend(title='Canal')
    g.set_axis_labels("Inversión en Ads (USD)", "Unidades Vendidas")
    g.set_titles("{col_name}")
    g.figure.suptitle("Impacto Publicitario por SKU y Canal", fontsize=13, y=1.02)
    plt.tight_layout()
    plt.show()

    # ── 5. VENTAS DIARIAS POR SKU ────────────────────────────────── [cite: 13]
    ventas_tiempo = df_eda.groupby(['fecha_venta', 'descripcion'])['cantidad_vendida'].sum().reset_index()

    plt.figure(figsize=(14, 5))
    for desc, group in ventas_tiempo.groupby('descripcion'):
        plt.plot(group['fecha_venta'], group['cantidad_vendida'], label=desc, linewidth=1.5)

    plt.title("Evolución de Ventas Diarias por Producto (2024)", fontsize=13)
    plt.xlabel("Fecha")
    plt.ylabel("Unidades Vendidas")
    plt.legend(title="Producto")
    plt.tight_layout()
    plt.show()

    print("\n✅ EDA completado.")
    print("⚙️ Aplicando Feature Engineering...")

    # Ordenar por canal, SKU y fecha para que los lags sean correctos [cite: 13]
    df_eda = df_eda.sort_values(['id_canal', 'id_sku', 'fecha_venta']).reset_index(drop=True)

    # 1. Lags de inversión publicitaria (por canal y SKU) [cite: 13]
    df_eda['inversion_usd_lag1'] = df_eda.groupby(['id_canal', 'id_sku'])['inversion_usd'].shift(1).fillna(0)
    df_eda['inversion_usd_lag2'] = df_eda.groupby(['id_canal', 'id_sku'])['inversion_usd'].shift(2).fillna(0)

    # 2. Día de la semana (0=lunes, 6=domingo) [cite: 13]
    df_eda['dia_semana'] = pd.to_datetime(df_eda['fecha_venta']).dt.dayofweek

    # 3. Verificar resultado [cite: 13, 14]
    print(f"✅ Feature Engineering completado: {df_eda.shape}")
    print(f"\nNuevas columnas agregadas:")
    print(df_eda[['fecha_venta', 'id_sku', 'id_canal', 'inversion_usd',
                  'inversion_usd_lag1', 'inversion_usd_lag2', 'dia_semana']].head(10).to_string())
    
    print("🚀 Subiendo feature_set_ml a Silver...")

    # Columnas finales que van al modelo [cite: 14]
    columnas_modelo = [
        'fecha_venta', 'id_sku', 'descripcion', 'categoria', 'id_canal',
        'lead_time_dias', 'cantidad_vendida', 'inversion_usd',
        'inversion_usd_lag1', 'inversion_usd_lag2', 'temperatura_promedio_celsius', 'dia_semana'
    ]

    df_silver_ml = df_eda[columnas_modelo].copy()

    # Convertir fecha a string para Supabase [cite: 14]
    df_silver_ml['fecha_venta'] = df_silver_ml['fecha_venta'].astype(str)

    # Reemplazar NaN por None para Supabase [cite: 14]
    df_silver_ml = df_silver_ml.replace({np.nan: None})

    # Subir en batches [cite: 14, 15]
    datos = df_silver_ml.to_dict(orient='records')
    batch_size = 500

    for i in range(0, len(datos), batch_size):
        try:
            supabase.schema('silver').table('feature_set_ml').insert(datos[i:i+batch_size]).execute()
        except Exception as e:
            print(f"❌ Error al cargar lote en feature_set_ml: {e}")

    print(f"✅ silver.feature_set_ml: {len(datos)} registros guardados.")
    print(f"\nResumen del dataset final:")
    print(f"   Registros : {df_silver_ml.shape[0]}")
    print(f"   Variables : {df_silver_ml.shape[1]}")
    print(f"   Período   : {df_silver_ml['fecha_venta'].min()} → {df_silver_ml['fecha_venta'].max()}")
    print(f"   SKUs      : {df_silver_ml['id_sku'].nunique()}")
    print(f"   Canales   : {df_silver_ml['id_canal'].nunique()}")

if __name__ == "__main__":
    generar_eda_y_feature_set()