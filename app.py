import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date, datetime
import gspread
from google.oauth2.service_account import Credentials
import json

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Consumos y rendimientos", page_icon="🚛", layout="wide")

DB_HOST = st.secrets["DB_HOST"]
DB_PORT = int(st.secrets["DB_PORT"])
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_NAME = st.secrets["DB_NAME"]

ADMIN_PASSWORD = st.secrets["ADMIN_PASSWORD"]

SHEETS_URL = st.secrets["SHEETS_URL"]
SHEETS_TAB = st.secrets["SHEETS_TAB"]

# =========================================================
# ESTILOS
# =========================================================
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

# =========================================================
# DB HELPERS
# =========================================================
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

# =========================================================
# DATA
# =========================================================
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
    df = run_select("""
        SELECT unidad, MAX(km_final) km
        FROM registro_diario
        GROUP BY unidad
    """)
    return {r["unidad"]: float(r["km"] or 0) for _, r in df.iterrows()}

@st.cache_data(ttl=300)
def limites():
    df = run_select("""
        SELECT region, tipo, modelo, limite_superior, limite_inferior
        FROM limites_rendimiento
    """)
    return {
        (r["region"], r["tipo"], r["modelo"]):
        (float(r["limite_inferior"]), float(r["limite_superior"]))
        for _, r in df.iterrows()
    }

# =========================================================
# INSERT
# =========================================================
def insertar_registros(filas):
    run_execute("""
        INSERT INTO registro_diario (
            fecha, region, plaza, unidad, tipo, modelo,
            km_inicial, km_final, km_recorridos,
            g_magna_l, g_magna_p,
            g_premium_l, g_premium_p,
            gas_l, gas_p,
            diesel_l, diesel_p,
            total_litros, total_importe,
            rendimiento_real, limite_superior, limite_inferior,
            hora_registro
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, filas)

# =========================================================
# GOOGLE SHEETS
# =========================================================
@st.cache_resource
def sheets_client():
    creds = Credentials.from_service_account_info(
        json.loads(st.secrets["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def enviar_sheets(filas):
    if not filas:
        return
    ws = sheets_client().open_by_url(SHEETS_URL).worksheet(SHEETS_TAB)
    ws.append_rows(filas, value_input_option="USER_ENTERED")

# =========================================================
# ADMIN
# =========================================================
with st.sidebar:
    st.header("🔐 Admin")
    if st.text_input("Contraseña", type="password") == ADMIN_PASSWORD:
        st.markdown(
            f'<a href="{SHEETS_URL}" target="_blank">'
            f'<button class="admin-button">📄 Abrir Google Sheets</button></a>',
            unsafe_allow_html=True
        )
        st.stop()

# =========================================================
# UI
# =========================================================
st.title("CONSUMOS Y RENDIMIENTOS 📈")

df = cargar_catalogo()
if df.empty:
    st.error("Catálogo vacío")
    st.stop()

# Región por URL
region_param = st.query_params.get("region")
if not region_param:
    st.error("Falta ?region=REGION")
    st.stop()

region_param = region_param.replace("_", " ").upper()
df["REGION_NORM"] = df["Region"].str.upper()

if region_param not in df["REGION_NORM"].unique():
    st.error(f"Región no válida: {region_param}")
    st.stop()

region = df[df["REGION_NORM"] == region_param]["Region"].iloc[0]

# Región / Plaza / Fecha
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
        st.error("Fecha futura no permitida")
        st.stop()

# Precios
p1, p2, p3, p4 = st.columns(4)
precio_gas = p1.number_input("Precio Gas $", 0.0)
precio_magna = p2.number_input("Precio Magna $", 0.0)
precio_premium = p3.number_input("Precio Premium $", 0.0)
precio_diesel = p4.number_input("Precio Diesel $", 0.0)

# =========================================================
# CAPTURA
# =========================================================
kms = ultimo_km()
lims = limites()

rows = []
for _, r in df[(df.Region == region) & (df.Plaza == plaza)].iterrows():
    km_ini = kms.get(r.Unidad, r["Km inicial"] or 0)
    rows.append({
        "Unidad": r.Unidad,
        "Km Final": None,
        "Gas (L)": 0.0,
        "Magna (L)": 0.0,
        "Premium (L)": 0.0,
        "Diesel (L)": 0.0,
        "_km": km_ini,
        "_tipo": r.Tipo,
        "_modelo": r.Modelo
    })

ed = st.data_editor(
    pd.DataFrame(rows),
    hide_index=True,
    column_config={"_km": None, "_tipo": None, "_modelo": None}
)

if st.button("GUARDAR"):
    filas_db = []
    filas_sh = []
    hora = datetime.now().strftime("%H:%M:%S")

    for _, x in ed.iterrows():

        km_final = pd.to_numeric(x["Km Final"], errors="coerce")
        km_ini = pd.to_numeric(x["_km"], errors="coerce")

        if pd.isna(km_final) or pd.isna(km_ini):
            continue

        if km_final <= km_ini:
            st.error(f"{x['Unidad']}: Km final <= Km inicial")
            continue

        gas = pd.to_numeric(x["Gas (L)"], errors="coerce") or 0
        magna = pd.to_numeric(x["Magna (L)"], errors="coerce") or 0
        premium = pd.to_numeric(x["Premium (L)"], errors="coerce") or 0
        diesel = pd.to_numeric(x["Diesel (L)"], errors="coerce") or 0

        litros = gas + magna + premium + diesel
        if litros <= 0:
            continue

        kmr = km_final - km_ini
        rend = kmr / litros
        li, ls = lims.get((region, x["_tipo"], x["_modelo"]), (None, None))

        filas_db.append((
            fecha, region, plaza, x.Unidad, x["_tipo"], x["_modelo"],
            km_ini, km_final, kmr,
            magna, magna * precio_magna,
            premium, premium * precio_premium,
            gas, gas * precio_gas,
            diesel, diesel * precio_diesel,
            litros,
            gas * precio_gas + magna * precio_magna + premium * precio_premium + diesel * precio_diesel,
            rend, ls, li, hora
        ))

        filas_sh.append(list(filas_db[-1]))

    if not filas_db:
        st.warning("No hubo registros válidos para guardar")
    else:
        insertar_registros(filas_db)
        enviar_sheets(filas_sh)
        st.success("✅ Guardado correctamente")
        st.rerun()












