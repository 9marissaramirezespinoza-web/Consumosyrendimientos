import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date, datetime
import gspread
from google.oauth2.service_account import Credentials
import json

st.set_page_config(page_title="Consumos y rendimientos", page_icon="🚛", layout="wide")

# ------------------ ESTILOS ------------------
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

# ------------------ CONFIG DESDE SECRETS ------------------
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = int(st.secrets.get("DB_PORT", 4000))
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_NAME = st.secrets["DB_NAME"]

LINK_EXCEL_NUBE = "https://docs.google.com/spreadsheets/d/1BHrjyuJcRhof5hp5VzjoGDzbB6i7olcp2mH8DkF3LwE/edit?hl=es&gid=0#gid=0"
HOJA_REGISTROS = "REGISTROS"
PASSWORD_ADMIN =  ""

# ------------------ DB ------------------
def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
)

def run_select(q, p=None):
    c = get_connection()
    df = pd.read_sql(q, c, params=p)
    c.close()
    return df

def run_execute(q, p=None, many=False):
    c = get_connection()
    cur = c.cursor()
    if many:
        cur.executemany(q, p)
    else:
        cur.execute(q, p)
    c.commit()
    cur.close()
    c.close()

# ------------------ DATA ------------------
@st.cache_data(ttl=300)
def cargar_catalogo():
    df = run_select("""
        SELECT region, plaza, unidad, tipo, modelo, anio, km_inicial
        FROM catalogo_unidades
    """)
    return df.rename(columns={
        "region":"Region","plaza":"Plaza","unidad":"Unidad",
        "tipo":"Tipo","modelo":"Modelo","anio":"Año","km_inicial":"Km inicial"
    })

@st.cache_data(ttl=300)
def ultimo_km():
    df = run_select("""
        SELECT unidad, MAX(km_final) km
        FROM registro_diario GROUP BY unidad
    """)
    return {r["unidad"]: float(r["km"] or 0) for _, r in df.iterrows()}

@st.cache_data(ttl=300)
def limites():
    df = run_select("""
        SELECT region,tipo,modelo,limite_superior,limite_inferior
        FROM limites_rendimiento
    """)
    return {(r["region"],r["tipo"],r["modelo"]):
            (float(r["limite_inferior"]),float(r["limite_superior"]))
            for _,r in df.iterrows()}

# ------------------ INSERT ------------------
def insertar(filas):
    run_execute("""
        INSERT INTO registro_diario (
        fecha,region,plaza,unidad,tipo,modelo,
        km_inicial,km_final,km_recorridos,
        g_magna_l,g_magna_p,
        g_premium_l,g_premium_p,
        gas_l,gas_p,
        diesel_l,diesel_p,
        total_litros,total_importe,
        rendimiento_real,limite_superior,limite_inferior,hora_registro)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, filas, many=True)

# ------------------ GOOGLE SHEETS ------------------
@st.cache_resource
def sheets_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        json.loads(st.secrets["GOOGLE_CREDENTIALS"]),
        scopes=scopes
    )
    return gspread.authorize(creds)

def enviar_sheets(filas):
    if not filas:
        return
    ws = sheets_client().open_by_url(LINK_EXCEL_NUBE).worksheet(HOJA_REGISTROS)
    ws.append_rows(filas, value_input_option="USER_ENTERED")

# ------------------ ADMIN ------------------
with st.sidebar:
    st.header("🔐 Admin")
    if st.text_input("Contraseña", type="password") == PASSWORD_ADMIN and PASSWORD_ADMIN:
        st.markdown(
            f'<a href="{LINK_EXCEL_NUBE}" target="_blank">'
            f'<button class="admin-button">📄 Abrir Google Sheets</button></a>',
            unsafe_allow_html=True
        )
        st.stop()

# ------------------ UI ------------------
st.title("CONSUMOS Y RENDIMIENTOS 📈")
df = cargar_catalogo()
if df.empty:
    st.error("Catálogo vacío")
    st.stop()

region_param = st.query_params.get("region")

if not region_param:
    st.error("Link inválido: falta ?region=...")
    st.stop()

# normalizamos
region_param = region_param.replace("_", " ").upper()

# normalizamos catálogo
df["Region_norm"] = df["Region"].str.upper()

if region_param not in df["Region_norm"].unique():
    st.error(f"Región no válida: {region_param}")
    st.stop()

# obtener región real
region = df[df["Region_norm"] == region_param]["Region"].iloc[0]


st.info(f"Región: **{region}**")

plaza = st.selectbox(
    "PLAZA",
    sorted(df[df["Region"]==region]["Plaza"].unique())
)

fecha = st.date_input("FECHA", date.today())
if fecha > date.today():
    st.stop()

# Precios
c1,c2,c3,c4 = st.columns(4)
precio_gas = c1.number_input("Gas $",0.0)
precio_magna = c2.number_input("Magna $",0.0)
precio_premium = c3.number_input("Premium $",0.0)
precio_diesel = c4.number_input("Diesel $",0.0)

# Tabla
kms = ultimo_km()
lims = limites()

rows=[]
for _,r in df[(df.Region==region)&(df.Plaza==plaza)].iterrows():
    km_ini = kms.get(r.Unidad, r["Km inicial"] or 0)
    rows.append({
        "Unidad":r.Unidad,"Km Final":None,
        "Gas":0.0,"Magna":0.0,"Premium":0.0,"Diesel":0.0,
        "_km":km_ini,"_tipo":r.Tipo,"_modelo":r.Modelo
    })

ed = st.data_editor(
    pd.DataFrame(rows),
    hide_index=True,
    column_config={
        "_tipo": None,
        "_modelo": None,
        "_km": None
    }
)

if st.button("GUARDAR"):
    filas_db=[]; filas_sh=[]
    for _,x in ed.iterrows():
        if not x["Km Final"]: 
            continue

        kmr = x["Km Final"] - x["_km"]
        litros = x.Gas + x.Magna + x.Premium + x.Diesel
        if litros <= 0:
            continue

        rend = kmr / litros
        li,ls = lims.get((region,x["_tipo"],x["_modelo"]),(None,None))

        fila = (
            fecha,region,plaza,x.Unidad,x["_tipo"],x["_modelo"],
            x["_km"],x["Km Final"],kmr,
            x.Magna,x.Magna*precio_magna,
            x.Premium,x.Premium*precio_premium,
            x.Gas,x.Gas*precio_gas,
            x.Diesel,x.Diesel*precio_diesel,
            litros,
            x.Gas*precio_gas+x.Magna*precio_magna+x.Premium*precio_premium+x.Diesel*precio_diesel,
            rend,ls,li,datetime.now().strftime("%H:%M:%S")
        )

        filas_db.append(fila)
        filas_sh.append(list(fila))

    insertar(filas_db)
    enviar_sheets(filas_sh)
    st.success("Guardado")
    st.rerun()







