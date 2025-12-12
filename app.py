import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date, datetime
import gspread
from google.oauth2.service_account import Credentials
import json
import math

st.set_page_config(page_title="Consumos y rendimientos", page_icon="🚛", layout="wide")

# ------------------ SECRETS ------------------
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = int(st.secrets["DB_PORT"])
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_NAME = st.secrets["DB_NAME"]

SHEETS_URL = "https://docs.google.com/spreadsheets/d/1BHrjyuJcRhof5hp5VzjoGDzbB6i7olcp2mH8DkF3LwE/edit"
SHEETS_TAB = "REGISTROS"
PASSWORD_ADMIN = "tec123"

# ------------------ DB ------------------
def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def run_select(q):
    c = get_connection()
    df = pd.read_sql(q, c)
    c.close()
    return df

def run_execute(q, params):
    c = get_connection()
    cur = c.cursor()
    cur.executemany(q, params)
    c.commit()
    cur.close()
    c.close()

# ------------------ DATA ------------------
@st.cache_data(ttl=300)
def cargar_catalogo():
    df = run_select("""
        SELECT region, plaza, unidad, tipo, modelo, km_inicial
        FROM catalogo_unidades
    """)
    return df

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

# ------------------ INSERT ------------------
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

# ------------------ GOOGLE SHEETS ------------------
@st.cache_resource
def sheets_client():
    creds = Credentials.from_service_account_info(
        json.loads(st.secrets["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def enviar_sheets(filas):
    if filas:
        ws = sheets_client().open_by_url(SHEETS_URL).worksheet(SHEETS_TAB)
        ws.append_rows(filas, value_input_option="USER_ENTERED")

# ------------------ ADMIN ------------------
with st.sidebar:
    st.header("🔐 Admin")
    if st.text_input("Contraseña", type="password") == PASSWORD_ADMIN:
        st.link_button("📄 Abrir Google Sheets", SHEETS_URL)
        st.stop()

# ------------------ UI ------------------
st.title("CONSUMOS Y RENDIMIENTOS 📈")

df = cargar_catalogo()
kms = ultimo_km()
lims = limites()

# Región por link
region_param = st.query_params.get("region")
if not region_param:
    st.error("Falta ?region=REGION_SUR")
    st.stop()

region_param = region_param.replace("_", " ").upper()
df["REG_NORM"] = df["region"].str.upper()

if region_param not in df["REG_NORM"].unique():
    st.error("Región no válida")
    st.stop()

region = df[df["REG_NORM"] == region_param]["region"].iloc[0]

c1, c2, c3 = st.columns(3)
with c1:
    st.info(region)
with c2:
    plaza = st.selectbox("PLAZA", sorted(df[df.region == region].plaza.unique()))
with c3:
    fecha = st.date_input("FECHA", date.today())

# Precios
p1,p2,p3,p4 = st.columns(4)
precio_gas = p1.number_input("Gas $", 0.0)
precio_magna = p2.number_input("Magna $", 0.0)
precio_premium = p3.number_input("Premium $", 0.0)
precio_diesel = p4.number_input("Diesel $", 0.0)

# Tabla
rows = []
for _, r in df[(df.region == region) & (df.plaza == plaza)].iterrows():
    km_ini = kms.get(r.unidad, r.km_inicial or 0)
    rows.append({
        "Unidad": r.unidad,
        "Km Final": "",
        "Gas (L)": 0.0,
        "Magna (L)": 0.0,
        "Premium (L)": 0.0,
        "Diesel (L)": 0.0,
        "_km": km_ini,
        "_tipo": r.tipo,
        "_modelo": r.modelo
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

        if pd.isna(x["Km Final"]) or x["Km Final"] == "":
            continue

        km_final = float(x["Km Final"])
        km_ini = float(x["_km"])

        if km_final <= km_ini:
            continue

        gas = float(x["Gas (L)"] or 0)
        magna = float(x["Magna (L)"] or 0)
        premium = float(x["Premium (L)"] or 0)
        diesel = float(x["Diesel (L)"] or 0)

        litros = gas + magna + premium + diesel
        if litros <= 0:
            continue

        kmr = km_final - km_ini
        rend = kmr / litros

        li, ls = lims.get((region, x["_tipo"], x["_modelo"]), (None, None))

        fila = (
            fecha, region, plaza, x["Unidad"], x["_tipo"], x["_modelo"],
            km_ini, km_final, kmr,
            magna, magna * precio_magna,
            premium, premium * precio_premium,
            gas, gas * precio_gas,
            diesel, diesel * precio_diesel,
            litros,
            gas * precio_gas + magna * precio_magna + premium * precio_premium + diesel * precio_diesel,
            rend, ls, li, hora
        )

        filas_db.append(fila)
        filas_sh.append(list(fila))

    if filas_db:
        insertar_registros(filas_db)
        enviar_sheets(filas_sh)
        st.success("✅ Guardado correctamente")
        st.rerun()
    else:
        st.warning("No hubo registros válidos para guardar")










