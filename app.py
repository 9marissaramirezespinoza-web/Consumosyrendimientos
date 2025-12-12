import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date, datetime
import gspread
from google.oauth2.service_account import Credentials
import json
import os

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

# ------------------ CONFIG (SECRETS) ------------------
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = int(st.secrets.get("DB_PORT", 4000))
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_NAME = st.secrets["DB_NAME"]

# Google Sheets (dejas tu link fijo como lo tienes)
LINK_EXCEL_NUBE = "https://docs.google.com/spreadsheets/d/1BHrjyuJcRhof5hp5VzjoGDzbB6i7olcp2mH8DkF3LwE/edit?hl=es&gid=0#gid=0"
HOJA_REGISTROS = "REGISTROS"

# Admin (fijo como lo tienes)
PASSWORD_ADMIN = "tec123"

# ------------------ SSL CA (opcional) ------------------
def get_ssl_ca_path():
    """
    Si tienes el PEM en secrets como TIDB_SSL_CA_PEM, lo escribimos a /tmp/...
    Si no existe, conectamos sin ssl_ca.
    """
    pem = st.secrets.get("TIDB_SSL_CA_PEM", "")
    pem = (pem or "").strip()
    if not pem:
        return None
    path = "/tmp/tidb_ca.pem"
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(pem)
    return path

# ------------------ DB ------------------
def get_connection():
    ssl_ca_path = get_ssl_ca_path()
    kwargs = dict(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    if ssl_ca_path:
        kwargs["ssl_ca"] = ssl_ca_path
    return mysql.connector.connect(**kwargs)

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
        ORDER BY region, plaza, unidad
    """)
    return df.rename(columns={
        "region": "Region",
        "plaza": "Plaza",
        "unidad": "Unidad",
        "tipo": "Tipo",
        "modelo": "Modelo",
        "anio": "Año",
        "km_inicial": "Km inicial"
    })

@st.cache_data(ttl=300)
def ultimo_km():
    df = run_select("""
        SELECT unidad, MAX(km_final) km
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
    lims = {}
    for _, r in df.iterrows():
        key = (str(r["region"]).strip(), str(r["tipo"]).strip(), str(r["modelo"]).strip())
        lim_inf = float(r["limite_inferior"]) if r["limite_inferior"] is not None else None
        lim_sup = float(r["limite_superior"]) if r["limite_superior"] is not None else None
        lims[key] = (lim_inf, lim_sup)
    return lims

# ------------------ INSERT ------------------
def insertar(filas):
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
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s
        )
    """, filas, many=True)

# ------------------ GOOGLE SHEETS ------------------
@st.cache_resource
def sheets_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_info = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def enviar_sheets(filas):
    if not filas:
        return
    ws = sheets_client().open_by_url(LINK_EXCEL_NUBE).worksheet(HOJA_REGISTROS)
    ws.append_rows(filas, value_input_option="USER_ENTERED")

def fmt_num(x, dec=2):
    if x is None or x == "" or (isinstance(x, float) and pd.isna(x)):
        return ""
    try:
        x = float(x)
    except:
        return x
    if float(x).is_integer():
        return int(x)
    return round(x, dec)

# ------------------ ADMIN ------------------
with st.sidebar:
    st.header("🔐 Admin")
    pwd = st.text_input("Contraseña", type="password")
    if (pwd == PASSWORD_ADMIN) and PASSWORD_ADMIN:
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

# -------- región fija por link --------
region_param = st.query_params.get("region")
if not region_param:
    st.error("Link inválido: falta ?region=...  (ej: ?region=REGION_SUR)")
    st.stop()

# normalizar param: "REGION_SUR" -> "REGION SUR"
region_norm = str(region_param).replace("_", " ").strip().upper()

# normalizar catálogo
df["Region_norm"] = df["Region"].astype(str).str.strip().str.upper()

if region_norm not in df["Region_norm"].unique():
    st.error(f"Región no válida: {region_norm}")
    st.stop()

# obtener valor real como viene en catálogo
region = df[df["Region_norm"] == region_norm]["Region"].iloc[0]

# -------- región / plaza / fecha en columnas --------
c1, c2, c3 = st.columns(3)

with c1:
    st.info(f"REGIÓN\n\n**{region}**")

with c2:
    plazas = sorted(df[df["Region"] == region]["Plaza"].dropna().unique().tolist())
    plaza = st.selectbox("PLAZA", plazas)

with c3:
    fecha = st.date_input("FECHA", value=date.today())
    if fecha > date.today():
        st.error("❌ No puedes capturar fecha futura")
        st.stop()

# -------- precios (solo cuando ya hay plaza) --------
st.subheader("Precios del día (capturados manualmente)")
p1, p2, p3, p4 = st.columns(4)
with p1:
    precio_gas = st.number_input("Precio Gas ($/L)", min_value=0.0, step=0.01, value=0.0)
with p2:
    precio_magna = st.number_input("Precio Gasolina Magna ($/L)", min_value=0.0, step=0.01, value=0.0)
with p3:
    precio_premium = st.number_input("Precio Gasolina Premium ($/L)", min_value=0.0, step=0.01, value=0.0)
with p4:
    precio_diesel = st.number_input("Precio Diesel ($/L)", min_value=0.0, step=0.01, value=0.0)

st.divider()

# -------- tabla captura --------
kms = ultimo_km()
lims = limites()

df_plaza = df[(df["Region"] == region) & (df["Plaza"] == plaza)].copy()

rows = []
for _, r in df_plaza.iterrows():
    unidad = str(r["Unidad"]).strip()
    if not unidad:
        continue

    km_base = r.get("Km inicial", 0) or 0
    km_ini = kms.get(unidad, km_base)

    rows.append({
        "Unidad": unidad,
        "Km Final": None,

        "Gas (L)": 0.0,
        "Gasolina Magna (L)": 0.0,
        "Gasolina Premium (L)": 0.0,
        "Diesel (L)": 0.0,

        "_km_ini": float(km_ini),
        "_tipo": str(r.get("Tipo", "") or "").strip(),
        "_modelo": str(r.get("Modelo", "") or "").strip(),
    })

df_editor = pd.DataFrame(rows)

ed = st.data_editor(
    df_editor,
    hide_index=True,
    use_container_width=True,
    height=520,
    column_order=[
        "Unidad", "Km Final",
        "Gas (L)", "Gasolina Magna (L)", "Gasolina Premium (L)", "Diesel (L)"
    ],
    column_config={
        "_tipo": None,
        "_modelo": None,
        "_km_ini": None,
    }
)

if st.button("✅ GUARDAR"):
    filas_db = []
    filas_sh = []
    errores = []
    hora = datetime.now().strftime("%H:%M:%S")

    # refrescar históricos por si alguien capturó al mismo tiempo
    historicos = ultimo_km()
    lims_dict = lims

    for _, x in ed.iterrows():
        unidad = str(x.get("Unidad", "")).strip()
        if not unidad:
            continue

        # --- validar KM FINAL ---
        if pd.isna(x.get("Km Final")) or str(x.get("Km Final")).strip() == "":
            continue  # no capturó esta unidad

        try:
            km_final = float(x["Km Final"])
            km_ini = float(historicos.get(unidad, x["_km_ini"]))
        except:
            errores.append(f"{unidad}: km inválido.")
            continue

        if km_final <= km_ini:
            errores.append(f"{unidad}: Km final ({km_final}) menor o igual a Km inicial ({km_ini}).")
            continue

        km_rec = km_final - km_ini

        if km_rec > 1400:
            errores.append(f"{unidad}: recorrido {km_rec:.0f} km mayor a 1400. Verifica.")
            continue

        # --- litros opcionales: NaN => 0 ---
        def num0(v):
            if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
                return 0.0
            try:
                return float(v)
            except:
                return 0.0

        gas_l = num0(x.get("Gas (L)"))
        magna_l = num0(x.get("Gasolina Magna (L)"))
        premium_l = num0(x.get("Gasolina Premium (L)"))
        diesel_l = num0(x.get("Diesel (L)"))

        total_litros = gas_l + magna_l + premium_l + diesel_l
        if total_litros <= 0:
            errores.append(f"{unidad}: no capturó litros (Gas/Magna/Premium/Diesel).")
            continue

        rendimiento = km_rec / total_litros

        tipo = str(x.get("_tipo", "") or "").strip()
        modelo = str(x.get("_modelo", "") or "").strip()

        # importes
        gas_p = gas_l * precio_gas
        g_magna_p = magna_l * precio_magna
        g_premium_p = premium_l * precio_premium
        diesel_p = diesel_l * precio_diesel
        total_importe = gas_p + g_magna_p + g_premium_p + diesel_p

        # límites
        lim_inf = None
        lim_sup = None
        key = (str(region).strip(), tipo, modelo)
        if key in lims_dict:
            lim_inf, lim_sup = lims_dict[key]

        # DB
        filas_db.append((
            fecha.strftime("%Y-%m-%d"),
            region,
            plaza,
            unidad,
            tipo,
            modelo,
            km_ini,
            km_final,
            km_rec,
            magna_l, g_magna_p,
            premium_l, g_premium_p,
            gas_l, gas_p,
            diesel_l, diesel_p,
            total_litros,
            total_importe,
            rendimiento,
            lim_sup, lim_inf,
            hora
        ))

        # Sheets (sin ID)
        filas_sh.append([
            fecha.strftime("%Y-%m-%d"),
            region,
            plaza,
            unidad,
            tipo,
            modelo,
            fmt_num(km_ini, 0),
            fmt_num(km_final, 0),
            fmt_num(km_rec, 0),

            fmt_num(gas_l, 2),
            fmt_num(gas_p, 2),

            fmt_num(magna_l, 2),
            fmt_num(g_magna_p, 2),

            fmt_num(premium_l, 2),
            fmt_num(g_premium_p, 2),

            fmt_num(diesel_l, 2),
            fmt_num(diesel_p, 2),

            fmt_num(total_litros, 3),
            fmt_num(total_importe, 2),
            fmt_num(rendimiento, 3),

            fmt_num(lim_sup, 3) if lim_sup is not None else "",
            fmt_num(lim_inf, 3) if lim_inf is not None else "",

            hora
        ])

    if errores:
        for e in errores:
            st.error(e)
        st.stop()

    if not filas_db:
        st.warning("No hay filas válidas para guardar.")
        st.stop()

    # Guardar TiDB
    try:
        insertar(filas_db)
        st.success("✅ Guardado en TiDB.")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"❌ Error guardando en TiDB: {e}")
        st.stop()

    # Guardar Sheets (best effort)
    try:
        enviar_sheets(filas_sh)
    except Exception as e:
        st.warning(f"⚠ No se pudo respaldar en Google Sheets: {e}")

    st.balloons()
    st.rerun()





















