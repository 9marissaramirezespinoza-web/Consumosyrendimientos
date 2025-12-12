import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date, datetime

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

def run_select(query, params=None):
    conn = get_connection()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

def run_execute(query, params, many=False):
    conn = get_connection()
    cur = conn.cursor()
    if many:
        cur.executemany(query, params)
    else:
        cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()

# ================== DATA ==================
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
        SELECT unidad, MAX(km_final) AS km
        FROM registro_diario
        GROUP BY unidad
    """)
    return {str(r["unidad"]): float(r["km"] or 0) for _, r in df.iterrows()}

@st.cache_data(ttl=300)
def limites():
    df = run_select("""
        SELECT region, tipo, modelo, limite_superior, limite_inferior
        FROM limites_rendimiento
    """)
    return {
        (str(r["region"]), str(r["tipo"]), str(r["modelo"])):
        (float(r["limite_inferior"]), float(r["limite_superior"]))
        for _, r in df.iterrows()
    }

# ================== INSERT ==================
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
    """, filas, many=True)

# ================== ADMIN ==================
with st.sidebar:
    st.header("🔐 Admin")
    if st.text_input("Contraseña", type="password") == PASSWORD_ADMIN:
        st.success("Modo administrador")
        st.stop()

# ================== UI ==================
st.title("CONSUMOS Y RENDIMIENTOS 📈")
if st.session_state.guardado_ok:
    st.success("✅ Guardado correctamente")
    st.session_state.guardado_ok = False

df = cargar_catalogo()
if df.empty:
    st.error("Catálogo vacío")
    st.stop()

# -------- Región por link --------
region_param = st.query_params.get("region")
if not region_param:
    st.error("Link inválido: falta ?region=REGION_SUR")
    st.stop()

region_param = region_param.replace("_", " ").upper()
df["REGION_NORM"] = df["Region"].str.upper()

if region_param not in df["REGION_NORM"].unique():
    st.error(f"Región no válida: {region_param}")
    st.stop()

region = df[df["REGION_NORM"] == region_param]["Region"].iloc[0]

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
        st.stop()

# -------- Precios --------
p1, p2, p3, p4 = st.columns(4)
precio_gas = p1.number_input("Precio Gas $", 0.0)
precio_magna = p2.number_input("Precio Magna $", 0.0)
precio_premium = p3.number_input("Precio Premium $", 0.0)
precio_diesel = p4.number_input("Precio Diesel $", 0.0)

# ================== CAPTURA ==================
kms = ultimo_km()
lims = limites()

rows = []
for _, r in df[(df.Region == region) & (df.Plaza == plaza)].iterrows():
    unidad = str(r.Unidad)
    km_ini = kms.get(unidad, r["Km inicial"] or 0)

    rows.append({
        "Unidad": unidad,
        "Km Final": "",
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
    column_config={
        "_km": None,
        "_tipo": None,
        "_modelo": None
    }
)

# ================== GUARDAR ==================
if st.button("GUARDAR"):
    filas_db = []
    hora = datetime.now().strftime("%H:%M:%S")

    for _, x in ed.iterrows():
        try:
            km_final = float(x["Km Final"])
            km_ini = float(x["_km"])
        except:
            continue

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

        total_importe = (
            gas * precio_gas +
            magna * precio_magna +
            premium * precio_premium +
            diesel * precio_diesel
        )

        fila = (
            fecha, region, plaza, x["Unidad"], x["_tipo"], x["_modelo"],
            km_ini, km_final, kmr,
            magna, magna * precio_magna,
            premium, premium * precio_premium,
            gas, gas * precio_gas,
            diesel, diesel * precio_diesel,
            litros, total_importe,
            rend, ls, li, hora
        )

        filas_db.append(fila)

    if filas_db:
        insertar_registros(filas_db)
      if filas_db:
    try:
        insertar_registros(filas_db)
        st.session_state.guardado_ok = True
    except Exception as e:
        st.error("❌ Error al guardar en TiDB")
        st.stop()

    # Google Sheets NO debe romper la app
    try:
        enviar_sheets(filas_sh)
    except Exception:
        pass

    st.rerun()
else:
    st.warning("No hubo registros válidos para guardar")



















