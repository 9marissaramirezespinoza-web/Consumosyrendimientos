import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date, datetime
import gspread
from google.oauth2.service_account import Credentials
import json

# ================== SESSION STATE ==================
if "guardado_ok" not in st.session_state:
    st.session_state.guardado_ok = False
if "sheets_error" not in st.session_state:
    st.session_state.sheets_error = None

# ================== CONFIG ==================
st.set_page_config(
    page_title="Consumos y rendimientos",
    page_icon="🚛",
    layout="wide"
)

# ================== ESTILOS ==================
st.markdown("""
<style>
.block-container { padding-top: 1rem; }
.admin-button {
    background-color: #1DB954;
    color: white;
    padding: 10px;
    border-radius: 6px;
    width: 100%;
    font-size: 15px;
    font-weight: bold;
    border: none;
}
</style>
""", unsafe_allow_html=True)

# ================== SECRETS ==================
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = int(st.secrets["DB_PORT"])
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_NAME = st.secrets["DB_NAME"]

SHEETS_URL = st.secrets.get("SHEETS_URL", "")
SHEETS_TAB = st.secrets.get("SHEETS_TAB", "REGISTROS")

PASSWORD_ADMIN = "tec123"

# ================== DB CONNECTION & EXECUTION ==================
def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def run_select(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def run_execute(query, params):
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(query, params)
    conn.commit()
    cur.close()
    conn.close()

# ================== DATA FETCH & CACHE ==================

def normalize_key(value):
    """Normaliza una cadena a MAYÚSCULAS y elimina espacios para un lookup seguro."""
    if value is not None:
        return str(value).strip().upper()
    return ""

@st.cache_data(ttl=300)
def cargar_catalogo():
    df = run_select("""
        SELECT region, plaza, unidad, tipo, modelo, km_inicial
        FROM catalogo_unidades
    """)
    return df.rename(columns={
        "region": "Region", "plaza": "Plaza", "unidad": "Unidad",
        "tipo": "Tipo", "modelo": "Modelo", "km_inicial": "Km inicial"
    })

@st.cache_data(ttl=300)
def ultimo_km():
    df = run_select("""
        SELECT unidad, MAX(km_final) AS km
        FROM registro_diario
        GROUP BY unidad
    """)
    # Usamos str() para la unidad para asegurar que la clave del diccionario sea consistente
    return {str(r["unidad"]): float(r["km"] or 0) for _, r in df.iterrows()}

@st.cache_data(ttl=300)
def limites():
    df = run_select("""
        SELECT region, tipo, modelo, limite_superior, limite_inferior
        FROM limites_rendimiento
    """)
    # Normalizamos las claves al cargarlas para la búsqueda (Problema C)
    return {
        (
            normalize_key(r["region"]),
            normalize_key(r["tipo"]),
            normalize_key(r["modelo"])
        ):
        (float(r["limite_inferior"] or 0), float(r["limite_superior"] or 0))
        for _, r in df.iterrows()
    }


# ================== INSERT EN DB ==================
def insertar_registros(filas):
    """Inserta las filas en TiDB Cloud."""
    run_execute("""
        INSERT INTO registro_diario (
            fecha, region, plaza, unidad, tipo, modelo,
            km_inicial, km_final, km_recorridos,
            gas_l, gas_p,
            g_magna_l, g_magna_p,
            g_premium_l, g_premium_p,
            diesel_l, diesel_p,
            total_litros, total_importe,
            rendimiento_real,
            limite_superior, limite_inferior,
            hora_registro
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,
            %s,%s,
            %s
        )
    """, filas)

# ================== GOOGLE SHEETS (BEST EFFORT) ==================

# ================== GOOGLE SHEETS (BEST EFFORT) - CORRECCIÓN DE PRECISIÓN ==================

def clean_for_sheets(value):
    """Convierte tipos no serializables (date, None) a string/float para Sheets,
       y redondea los floats para evitar el problema de precisión excesiva."""
    
    if isinstance(value, (date, datetime)):
        # Si es una fecha u hora, la convierte a formato ISO (ej. 2025-12-13)
        return value.isoformat()
    
    elif value is None:
        # Si es None (como los límites que pueden ser nulos), devuelve un string vacío
        return ""
    
    # *** NUEVA LÍNEA CRÍTICA: Redondeo de Floats ***
    elif isinstance(value, float):
        # Redondeamos a 3 decimales para mantener la precisión de rendimiento y precios.
        # Esto resuelve el problema de la cantidad "rarísima" en Google Sheets.
        return round(value, 3) 
        
    # Para cualquier otro tipo (int, str que no sea None), lo deja como está o lo convierte a string
    return str(value) 

# La función enviar_sheets no necesita cambios internos adicionales.
def enviar_sheets(filas):
    if not filas or not SHEETS_URL:
        return
    # ... (el resto de la función usa clean_for_sheets en la limpieza de filas)
    try:
        creds_content = st.secrets["GOOGLE_CREDENTIALS"]
        
        # Manejo de la carga de credenciales (TOML/JSON)
        if isinstance(creds_content, str):
             creds_dict = json.loads(creds_content)
        else:
            creds_dict = creds_content

        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        ws = client.open_by_url(SHEETS_URL).worksheet(SHEETS_TAB)
        
        # Limpieza de datos antes de enviar (Solución a TypeError)
        filas_limpias = [
            [clean_for_sheets(value) for value in fila] 
            for fila in filas
        ]

        ws.append_rows(filas_limpias, value_input_option="USER_ENTERED")
        
    except Exception as e:
        st.session_state.sheets_error = f"Sheets Falló: {e}"
        pass  

# ================== ADMIN ==================
with st.sidebar:
    st.header("🔐 Admin")
    if st.text_input("Contraseña", type="password") == PASSWORD_ADMIN:
        if SHEETS_URL:
            st.markdown(
                f'<a href="{SHEETS_URL}" target="_blank">'
                f'<button class="admin-button">📄 Abrir Google Sheets</button></a>',
                unsafe_allow_html=True
            )
        st.stop()

# ================== UI ==================
st.title("CONSUMOS Y RENDIMIENTOS 📈")

# MENSAJE POST-GUARDADO (Solución a Problema B)
if st.session_state.guardado_ok:
    st.success("✅ Guardado correctamente en la base de datos.")
    
    # Muestra el error de Sheets si ocurrió
    if st.session_state.get("sheets_error"):
         st.warning(f"⚠️ Atención: TiDB guardó, pero la sincronización con Sheets falló: {st.session_state.sheets_error}")
         del st.session_state.sheets_error

    st.session_state.guardado_ok = False


df = cargar_catalogo()
if df.empty:
    st.error("Catálogo vacío")
    st.stop()

# -------- Región por link --------
region_param = st.query_params.get("region")
if not region_param:
    st.error("Link inválido: falta ?region=REGION_SUR.")
    st.stop()

# Normalización para búsqueda de región
region_param_norm = normalize_key(region_param)
df["REGION_NORM"] = df["Region"].apply(normalize_key)

if region_param_norm not in df["REGION_NORM"].unique():
    st.error(f"Región no válida en el link: {region_param}")
    st.stop()

# Obtenemos la versión original de la región para la UI y la DB
region = df[df["REGION_NORM"] == region_param_norm]["Region"].iloc[0]

# -------- Región / Plaza / Fecha --------
c1, c2, c3 = st.columns(3)
with c1:
    st.info(f"REGIÓN\n\n**{region}**")

with c2:
    plaza = st.selectbox(
        "PLAZA",
        sorted(df[df["Region"] == region]["Plaza"].unique())
    )

with c3:
    fecha = st.date_input("FECHA", date.today())
    if fecha > date.today():
        st.error("No se pueden registrar consumos en fechas futuras.")
        st.stop()

# -------- Precios (Se usan keywords como medida de robustez) --------
p1, p2, p3, p4 = st.columns(4)
precio_gas = p1.number_input(label="Precio Gas $", value=0.0, min_value=0.0)
precio_magna = p2.number_input(label="Precio Magna $", value=0.0, min_value=0.0)
precio_premium = p3.number_input(label="Precio Premium $", value=0.0, min_value=0.0)
precio_diesel = p4.number_input(label="Precio Diesel $", value=0.0, min_value=0.0)

# ================== CAPTURA ==================
kms = ultimo_km()
limites_dict = limites() 

rows = []
filtered_df = df[(df.Region == region) & (df.Plaza == plaza)]

for _, r in filtered_df.iterrows():
    unidad = str(r.Unidad)
    
    # **LÓGICA CORREGIDA KM INICIAL (Solución a Problema A)**
    km_previo = kms.get(unidad) 
    
    if km_previo is not None and km_previo > 0:
        km_ini = km_previo # Km final del día anterior (registro_diario)
    else:
        # Si no hay registros, usar el Km inicial base del catálogo
        km_ini = float(r["Km inicial"] or 0)
        
    rows.append({
        "Unidad": unidad,
        "Km Final": "",
        "Gas (L)": 0.0,
        "Magna (L)": 0.0,
        "Premium (L)": 0.0,
        "Diesel (L)": 0.0,
        # Campos ocultos
        "_km_ini": km_ini, 
        "_tipo": r.Tipo,
        "_modelo": r.Modelo
    })

ed = st.data_editor(
    pd.DataFrame(rows),
    hide_index=True,
    # Se asegura que la columna de Km inicial sea la correcta
    column_config={"_km_ini": None, "_tipo": None, "_modelo": None} 
)

# Contenedor para mostrar mensajes de error/warning específicos de la tabla
table_messages = st.container()

# ================== GUARDAR ==================
if st.button("GUARDAR"):
    filas_db = []
    filas_sh = []
    hora = datetime.now().strftime("%H:%M:%S")
    valid_records_count = 0
    
    for index, x in ed.iterrows():
        unidad = x["Unidad"]
        
        # --- 1. VALIDACIÓN DE FORMATO ---
        try:
            km_final = float(x["Km Final"])
            km_ini = float(x["_km_ini"])
        except:
            if x["Km Final"]: 
                table_messages.error(f"❌ Error en la unidad {unidad}: El campo 'Km Final' no es un número válido.")
                filas_db = [] 
                break 
            continue 

        # --- 2. VALIDACIÓN DE KM INICIAL/FINAL (Problema A) ---
        if km_final < km_ini:
            table_messages.warning(
                f"⚠️ Omisión en la unidad {unidad}: Km Final ({km_final}) debe ser estrictamente mayor que Km Inicial ({km_ini})."
            )
            continue
            
        gas = float(x["Gas (L)"] or 0)
        magna = float(x["Magna (L)"] or 0)
        premium = float(x["Premium (L)"] or 0)
        diesel = float(x["Diesel (L)"] or 0)

        litros = gas + magna + premium + diesel
        
        # --- 3. VALIDACIÓN DE LITROS ---
        if litros <= 0:
            table_messages.warning(
                f"⚠️ Omisión en la unidad {unidad}: Se registró kilometraje, pero no se capturaron litros válidos."
            )
            continue
            
        # El registro es VÁLIDO
        valid_records_count += 1 

        kmr = km_final - km_ini
        # --------------------------------------------------------
        # --- NUEVA VALIDACIÓN: KM EXCESIVO (Más de 1500 km) ---
        if kmr > 1500:
            # Muestra el error crítico y detiene toda la operación
            table_messages.error(
                f"❌ ERROR CRÍTICO en la unidad {unidad}: Kilometraje Recorrido ({kmr:,.0f} km) excede el límite de 1,500 km. "
                "Posible error de captura. La inserción completa se ha CANCELADO. Corrija el Km Final y vuelva a intentar."
            )
            filas_db = [] # Asegura que no se intente guardar ningún registro
            break # Sale inmediatamente del bucle
        # --------------------------------------------------------
        rend = kmr / litros
        
        # --- 4. OBTENCIÓN DE LÍMITES (Problema C) ---
        key = (normalize_key(region), normalize_key(x["_tipo"]), normalize_key(x["_modelo"]))
        lim_inf, lim_sup = limites_dict.get(key, (None, None))
        
        # --- CÁLCULO DE IMPORTE ---
        total_importe = (
            gas * precio_gas +
            magna * precio_magna +
            premium * precio_premium +
            diesel * precio_diesel
        )

        # --- CONSTRUCCIÓN DE LA FILA ---
        fila = (
            fecha, region, plaza, unidad, x["_tipo"], x["_modelo"],
            km_ini, km_final, kmr,
            gas, gas * precio_gas,
            magna, magna * precio_magna,
            premium, premium * precio_premium,
            diesel, diesel * precio_diesel,
            litros, total_importe,
            rend,
            lim_sup, lim_inf, # Los límites se obtienen aquí
            hora
        )

        filas_db.append(fila)
        filas_sh.append(list(fila))

    # 5. LÓGICA DE GUARDADO FINAL
    if filas_db:
        try:
            insertar_registros(filas_db)
            enviar_sheets(filas_sh)
            st.session_state.guardado_ok = True
            st.rerun()
        except Exception as e:
            table_messages.error(f"❌ Error crítico al guardar en TiDB: {e}. Reportar a soporte.")
    elif valid_records_count == 0:
        table_messages.warning("⚠️ No se encontró ningún registro válido para guardar. Revise que haya llenado Km Final y Litros.")





















