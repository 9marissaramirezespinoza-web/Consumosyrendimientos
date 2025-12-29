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
    if value is not None:
        return str(value).strip().upper()
    return ""

@st.cache_data(ttl=300)
def cargar_catalogo():
    df = run_select("""
        SELECT region, plaza, unidad, tipo, modelo, anio, km_inicial, limite_superior, limite_inferior
        FROM catalogo_unidades
    """)
    df = df.fillna(0)
    return df.rename(columns={
        "region": "Region", "plaza": "Plaza", "unidad": "Unidad",
        "tipo": "Tipo", "modelo": "Modelo", "anio": "Anio",
        "km_inicial": "Km inicial", "limite_superior": "lim_sup", "limite_inferior": "lim_inf"
    })

@st.cache_data(ttl=300)
def ultimo_km():
    df = run_select("""
        SELECT unidad, MAX(km_final) AS km
        FROM registro_diario
        GROUP BY unidad
    """)
    return {str(r["unidad"]): float(r["km"] or 0) for _, r in df.iterrows()}

def ya_hay_captura(reg, plz, fec):
    query = f"SELECT COUNT(*) as cuenta FROM registro_diario WHERE region = '{reg}' AND plaza = '{plz}' AND fecha = '{fec}'"
    df_check = run_select(query)
    return df_check["cuenta"].iloc[0] > 0

# ================== INSERT EN DB ==================
def insertar_registros(filas):
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
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, filas)

# ================== GOOGLE SHEETS ==================
def clean_for_sheets(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    elif value is None:
        return ""
    elif isinstance(value, float):
        return round(value, 3) 
    return str(value) 

def enviar_sheets(filas):
    if not filas or not SHEETS_URL:
        return
    try:
        creds_content = st.secrets["GOOGLE_CREDENTIALS"]
        creds_dict = json.loads(creds_content) if isinstance(creds_content, str) else creds_content
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        ws = client.open_by_url(SHEETS_URL).worksheet(SHEETS_TAB)
        filas_limpias = [[clean_for_sheets(v) for v in fila] for fila in filas]
        ws.append_rows(filas_limpias, value_input_option="USER_ENTERED")
    except Exception as e:
        st.session_state.sheets_error = f"Sheets Falló: {e}"

# ================== UI ==================
with st.sidebar:
    st.header("🔐 Admin")
    if st.text_input("Contraseña", type="password") == PASSWORD_ADMIN:
        if SHEETS_URL:
            st.markdown(f'<a href="{SHEETS_URL}" target="_blank"><button class="admin-button">📄 Abrir Sheets</button></a>', unsafe_allow_html=True)
        st.stop()

st.title("CONSUMOS Y RENDIMIENTOS 📈")

if st.session_state.guardado_ok:
    st.success("✅ Guardado correctamente en la base de datos.")
    if st.session_state.get("sheets_error"):
        st.warning(f"⚠️ TiDB guardó, pero Sheets falló: {st.session_state.sheets_error}")
        del st.session_state.sheets_error
    st.session_state.guardado_ok = False

df = cargar_catalogo()
region_param = st.query_params.get("region")
if not region_param:
    st.error("Link inválido.")
    st.stop()

region_param_norm = normalize_key(region_param)
df["REGION_NORM"] = df["Region"].apply(normalize_key)
region = df[df["REGION_NORM"] == region_param_norm]["Region"].iloc[0]

c1, c2, c3 = st.columns(3)
with c1: st.info(f"REGIÓN\n\n**{region}**")
with c2: plaza = st.selectbox("PLAZA", sorted(df[df["Region"] == region]["Plaza"].unique()))
with c3: fecha = st.date_input("FECHA", date.today())

if ya_hay_captura(region, plaza, fecha):
    st.info("🌟 **Gracias por capturar el día de hoy.**")
    st.stop()

p1, p2, p3, p4 = st.columns(4)
precio_gas = p1.number_input("Precio Gas $", value=0.0, min_value=0.0)
precio_magna = p2.number_input("Precio Magna $", value=0.0, min_value=0.0)
precio_premium = p3.number_input("Precio Premium $", value=0.0, min_value=0.0)
precio_diesel = p4.number_input("Precio Diesel $", value=0.0, min_value=0.0)

# ================== CAPTURA ==================
kms = ultimo_km()
filtered_df = df[(df.Region == region) & (df.Plaza == plaza)].copy()
try:
    filtered_df['Unidad_Num'] = filtered_df['Unidad'].str.replace(r'[^0-9]', '', regex=True).astype(int)
    filtered_df = filtered_df.sort_values(by='Unidad_Num', ascending=True)
except:
    filtered_df = filtered_df.sort_values(by='Unidad', ascending=True)

rows = []
for _, r in filtered_df.iterrows():
    unidad = str(r.Unidad)
    km_previo = kms.get(unidad) 
    km_ini = km_previo if km_previo is not None and km_previo > 0 else float(r["Km inicial"] or 0)
    rows.append({"Unidad": unidad, "Km Final": "", "Gas (L)": 0.0, "Magna (L)": 0.0, "Premium (L)": 0.0, "Diesel (L)": 0.0,
                 "_km_ini": km_ini, "_tipo": r.Tipo, "_modelo": r.Modelo, "_lim_sup": r.lim_sup, "_lim_inf": r.lim_inf})

ed = st.data_editor(pd.DataFrame(rows), hide_index=True, column_config={"_km_ini":None, "_tipo":None, "_modelo":None, "_lim_sup":None, "_lim_inf":None})
table_messages = st.container()

# ================== GUARDAR (LIBRE DE LÍMITES KM) ==================
if st.button("GUARDAR✅"):
    if precio_gas <= 0 or precio_magna <= 0 or precio_premium <= 0 or precio_diesel <= 0:
        table_messages.error("❌ ERROR: Ingrese todos los precios.")
        st.stop()

    filas_db, filas_sh = [], []
    hora = datetime.now().strftime("%H:%M:%S")
    has_critical_error = False
    
    for index, x in ed.iterrows():
        try:
            km_final = float(x["Km Final"])
            km_ini = float(x["_km_ini"])
        except:
            if x["Km Final"]: 
                table_messages.error(f"❌ Km Final inválido en {x['Unidad']}")
                has_critical_error = True
                break
            continue

        gas, magna, premium, diesel = [float(x[c] or 0) for c in ["Gas (L)", "Magna (L)", "Premium (L)", "Diesel (L)"]]
        litros = gas + magna + premium + diesel
        
        # Saltamos si no hay litros y no se movió el kilometraje
        if litros <= 0 and km_final == km_ini:
            continue

        # Si se movió pero no hay litros, error
        if litros <= 0 and km_final != km_ini:
            table_messages.error(f"❌ Error en {x['Unidad']}: Falta capturar litros.")
            has_critical_error = True
            break

        # CÁLCULOS SIN RESTRICCIONES DE 1500KM O KM MENOR
        kmr = km_final - km_ini
        rend = kmr / litros if litros > 0 else 0
        total_importe = (gas*precio_gas + magna*precio_magna + premium*precio_premium + diesel*precio_diesel)

        fila = (fecha, region, plaza, x["Unidad"], x["_tipo"], x["_modelo"], km_ini, km_final, kmr,
                gas, gas*precio_gas, magna, magna*precio_magna, premium, premium*precio_premium,
                diesel, diesel*precio_diesel, litros, total_importe, rend,
                x["_lim_sup"] if x["_lim_sup"] > 0 else None, 
                x["_lim_inf"] if x["_lim_inf"] > 0 else None, hora)

        filas_db.append(fila)
        filas_sh.append(list(fila))

    if filas_db and not has_critical_error:
        try:
            insertar_registros(filas_db)
            enviar_sheets(filas_sh)
            ultimo_km.clear()
            st.session_state.guardado_ok = True
            st.rerun()
        except Exception as e:
            table_messages.error(f"❌ Error en TiDB: {e}")
    elif not has_critical_error:
        table_messages.warning("⚠️ No hay datos para guardar.")
































