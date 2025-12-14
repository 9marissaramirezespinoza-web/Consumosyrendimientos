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
if "error_message" not in st.session_state:
    st.session_state.error_message = None

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
# Se asume que las claves DB_* son strings/ints
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = int(st.secrets["DB_PORT"])
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_NAME = st.secrets["DB_NAME"]

SHEETS_URL = st.secrets.get("SHEETS_URL", "")
SHEETS_TAB = st.secrets.get("SHEETS_TAB", "REGISTROS")

PASSWORD_ADMIN = "tec123"

# ================== DB ==================
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
    # Usamos read_sql_query para evitar el warning de pandas
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def run_execute(query, params):
    conn = get_connection()
    cur = conn.cursor()
    # Ejecuta el insert/update
    cur.executemany(query, params)
    conn.commit()
    cur.close()
    conn.close()

# ================== DATA FETCH ==================
# Se mantiene tu función de normalización de claves para que coincida con la DB
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
        "region": "Region",
        "plaza": "Plaza",
        "unidad": "Unidad",
        "tipo": "Tipo",
        "modelo": "Modelo",
        "km_inicial": "Km inicial"
    })

@st.cache_data(ttl=300)
def ultimo_km():
    """Devuelve el último km_final registrado por unidad."""
    df = run_select("""
        SELECT unidad, MAX(km_final) AS km
        FROM registro_diario
        GROUP BY unidad
    """)
    # Usamos str(r["unidad"]) para asegurar que coincida con el catálogo
    return {str(r["unidad"]): float(r["km"] or 0) for _, r in df.iterrows()}

@st.cache_data(ttl=300)
def limites():
    """Carga los límites y normaliza las claves para el lookup."""
    df = run_select("""
        SELECT region, tipo, modelo, limite_superior, limite_inferior
        FROM limites_rendimiento
    """)
    return {
        (
            normalize_key(r["region"]),
            normalize_key(r["tipo"]),
            normalize_key(r["modelo"])
        ):
        (float(r["limite_inferior"]), float(r["limite_superior"]))
        for _, r in df.iterrows()
    }


# ================== INSERT EN DB ==================
def insertar_registros(filas):
    """Inserta las filas en TiDB Cloud."""
    run_execute("""
        INSERT INTO registro_diario (
            fecha, region, plaza, unidad, tipo, modelo,
            km_inicial, km_final, km_recorridos,
            g_magna_l, g_magna_p,
            g_premium_l, g_premium_p,
            gas_l, gas_p,
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

def clean_for_sheets(value):
    """Convierte tipos no serializables (date, None) a string/float para Sheets."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    elif value is None:
        return ""
    # Se convierte todo a string por si hay Decimal o algún otro objeto
    return str(value) 

def enviar_sheets(filas):
    if not filas or not SHEETS_URL:
        return
    
    try:
        # **CORRECCIÓN:** Se asume que GOOGLE_CREDENTIALS está en formato TOML o JSON
        creds_content = st.secrets["GOOGLE_CREDENTIALS"]
        
        # Intentar cargar como string JSON (si es el formato legacy)
        if isinstance(creds_content, str):
             creds_dict = json.loads(creds_content)
        # Asumir que Streamlit lo cargó como dict (formato TOML preferido)
        else:
            creds_dict = creds_content

        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        ws = client.open_by_url(SHEETS_URL).worksheet(SHEETS_TAB)
        
        # Limpieza de datos antes de enviar
        filas_limpias = [
            [clean_for_sheets(value) for value in fila] 
            for fila in filas
        ]

        ws.append_rows(filas_limpias, value_input_option="USER_ENTERED")
        
    except Exception as e:
        # Guarda el error en sesión para mostrarlo luego, pero no rompe la app
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

# MENSAJE POST-GUARDADO (SIEMPRE SALE ARRIBA)
if st.session_state.guardado_ok:
    st.success("✅ Guardado correctamente en la base de datos.")
    
    # Muestra el error de Sheets si ocurrió
    if st.session_state.get("sheets_error"):
         st.warning(f"⚠️ Atención: TiDB guardó, pero la sincronización con Sheets falló. {st.session_state.sheets_error}")
         del st.session_state.sheets_error

    st.session_state.guardado_ok = False


df = cargar_catalogo()
if df.empty:
    st.error("Catálogo vacío")
    st.stop()

# -------- Región por link --------
region_param = st.query_params.get("region")
if not region_param:
    st.error("Link inválido: falta ?region=REGION_SUR. Contacta a soporte.")
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

# -------- Precios --------
p1, p2, p3, p4 = st.columns(4)
precio_gas = p1.number_input("Precio Gas $", 0.0, min_value=0.0)
precio_magna = p2.number_input("Precio Magna $", 0.0, min_value=0.0)
precio_premium = p3.number_input("Precio Premium $", 0.0, min_value=0.0)
precio_diesel = p4.number_input("Precio Diesel $", 0.0, min_value=0.0)

# ================== CAPTURA ==================
kms = ultimo_km()
limites_dict = limites() # Cargar límites para uso futuro

rows = []
filtered_df = df[(df.Region == region) & (df.Plaza == plaza)]

for _, r in filtered_df.iterrows():
    unidad = str(r.Unidad)
    
    # **CORRECCIÓN LÓGICA KM INICIAL**
    km_previo = kms.get(unidad) 
    
    if km_previo is not None and km_previo > 0:
        km_ini = km_previo # Usar Km final previo
    else:
        # Usar Km inicial del catálogo (asegurando float)
        km_ini = float(r["Km inicial"] or 0)
        
    rows.append({
        "Unidad": unidad,
        "Km Final": "",
        "Gas (L)": 0.0,
        "Magna (L)": 0.0,
        "Premium (L)": 0.0,
        "Diesel (L)": 0.0,
        # Campos ocultos para la lógica
        "_km_ini": km_ini, # <- Usamos un nombre claro
        "_tipo": r.Tipo,
        "_modelo": r.Modelo
    })

ed = st.data_editor(
    pd.DataFrame(rows),
    hide_index=True,
    # Se actualiza el nombre de la columna oculta
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
    
    # 1. Iterar sobre los datos capturados
    for index, x in ed.iterrows():
        unidad = x["Unidad"]
        
        # Validaciones de formato
        try:
            km_final = float(x["Km Final"])
            km_ini = float(x["_km_ini"])
        except:
            if x["Km Final"]: # Si el campo Km Final no está vacío, pero es inválido
                table_messages.error(f"❌ Error en la unidad {unidad}: El campo 'Km Final' debe ser un número válido.")
                filas_db = [] # Vaciar la lista para asegurar que no guarde nada
                break # Detener la ejecución de la inserción

            continue # Si el campo Km Final está vacío, simplemente lo omitimos

        # Validación 1: KM Final vs. KM Inicial
        if km_final <= km_ini:
            table_messages.warning(
                f"⚠️ Omisión en la unidad {unidad}: Km Final ({km_final}) debe ser estrictamente mayor que Km Inicial ({km_ini})."
            )
            continue
            
        gas = float(x["Gas (L)"] or 0)
        magna = float(x["Magna (L)"] or 0)
        premium = float(x["Premium (L)"] or 0)
        diesel = float(x["Diesel (L)"] or 0)

        litros = gas + magna + premium + diesel
        
        # Validación 2: Litros capturados > 0
        if litros <= 0:
            table_messages.warning(
                f"⚠️ Omisión en la unidad {unidad}: Se registró kilometraje, pero no se capturaron litros válidos."
            )
            continue
            
        # El registro es VÁLIDO
        valid_records_count += 1 

        kmr = km_final - km_ini
        rend = kmr / litros
        
        # --- Obtención de Límites (Corrección de Claves) ---
        key = (normalize_key(region), normalize_key(x["_tipo"]), normalize_key(x["_modelo"]))
        lim_inf, lim_sup = limites_dict.get(key, (None, None))
        
        # --- Cálculo de Importe ---
        total_importe = (
            gas * precio_gas +
            magna * precio_magna +
            premium * precio_premium +
            diesel * precio_diesel
        )

        # --- Construcción de la fila ---
        fila = (
            fecha, region, plaza, unidad, x["_tipo"], x["_modelo"],
            km_ini, km_final, kmr,
            magna, magna * precio_magna,
            premium, premium * precio_premium,
            gas, gas * precio_gas,
            diesel, diesel * precio_diesel,
            litros, total_importe,
            rend,
            lim_sup, lim_inf, # <-- Límites corregidos
            hora
        )

        filas_db.append(fila)
        filas_sh.append(list(fila)) # Convertir a lista para Sheets

    # 2. Lógica de guardado final
    if filas_db:
        try:
            insertar_registros(filas_db)
            enviar_sheets(filas_sh)
            st.session_state.guardado_ok = True
            st.rerun()
        except Exception as e:
            # Error crítico de la BD (conexión, permisos, etc.)
            table_messages.error(f"❌ Error crítico al guardar en TiDB: {e}. Contacta a soporte.")
    elif valid_records_count == 0:
        # Si el ciclo terminó sin errores de formato, pero sin registros válidos
        table_messages.warning("⚠️ No se encontró ningún registro válido para guardar. Revise que haya llenado Km Final y Litros.")


















