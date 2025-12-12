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
        h1, h2, h3 { word-break: break-word; }
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
# TiDB
DB_HOST = st.secrets["TIDB_HOST"]
DB_PORT = int(st.secrets.get("TIDB_PORT", 4000))
DB_USER = st.secrets["TIDB_USER"]
DB_PASSWORD = st.secrets["TIDB_PASSWORD"]
DB_NAME = st.secrets["TIDB_DATABASE"]

# Google Sheets
LINK_EXCEL_NUBE = st.secrets["SHEETS_URL"]
HOJA_REGISTROS = st.secrets.get("SHEETS_TAB", "REGISTROS")

# Admin
PASSWORD_ADMIN = st.secrets.get("ADMIN_PASSWORD", "")

# ------------------ SSL CA (crear archivo desde secrets) ------------------
def get_ssl_ca_path() -> str:
    """
    Streamlit Cloud no trae tu .pem si no lo subes.
    Aquí lo generamos desde st.secrets["TIDB_SSL_CA_PEM"].
    """
    pem_text = st.secrets.get("TIDB_SSL_CA_PEM", "").strip()
    if not pem_text:
        return ""  # si tu TiDB no lo requiere, puedes dejarlo vacío

    path = "/tmp/isrgrootx1.pem"
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(pem_text)
    return path

# ------------------ DB HELPERS (TiDB) ------------------
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

def run_select(query, params=None):
    conn = get_connection()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

def run_execute(query, params=None, many=False):
    conn = get_connection()
    cur = conn.cursor()
    if many and isinstance(params, list):
        cur.executemany(query, params)
    else:
        cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()

@st.cache_data(ttl=300)
def cargar_catalogo_desde_db():
    query = """
        SELECT region, plaza, unidad, tipo, modelo, anio, km_inicial
        FROM catalogo_unidades
        ORDER BY region, plaza, unidad
    """
    df = run_select(query)
    df = df.rename(columns={
        "region": "Region",
        "plaza": "Plaza",
        "unidad": "Unidad",
        "tipo": "Tipo",
        "modelo": "Modelo",
        "anio": "Año",
        "km_inicial": "Km inicial"
    })
    return df

@st.cache_data(ttl=300)
def obtener_ultimo_km_por_unidad_db():
    query = """
        SELECT unidad, MAX(km_final) AS ultimo_km
        FROM registro_diario
        GROUP BY unidad
    """
    df = run_select(query)
    resultado = {}
    for _, row in df.iterrows():
        unidad = str(row["unidad"])
        ultimo_km = row["ultimo_km"] if row["ultimo_km"] is not None else 0
        resultado[unidad] = float(ultimo_km)
    return resultado

@st.cache_data(ttl=300)
def cargar_limites_rendimiento():
    query = """
        SELECT region, tipo, modelo, limite_superior, limite_inferior
        FROM limites_rendimiento
    """
    df = run_select(query)
    limites = {}
    for _, row in df.iterrows():
        key = (
            str(row["region"]).strip(),
            str(row["tipo"]).strip(),
            str(row["modelo"]).strip()
        )
        lim_sup = float(row["limite_superior"])
        lim_inf = float(row["limite_inferior"])
        limites[key] = (lim_inf, lim_sup)
    return limites

def insertar_registros_diarios(filas):
    query = """
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
    """
    run_execute(query, filas, many=True)

# ------------------ GOOGLE SHEETS HELPERS ------------------
@st.cache_resource
def get_gspread_client():
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_json = st.secrets["GOOGLE_CREDENTIALS_JSON"]
        creds_info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.warning(f"⚠ No se pudo inicializar Google Sheets: {e}")
        return None

def obtener_worksheet_por_nombre(name):
    client = get_gspread_client()
    if not client:
        return None
    try:
        ss = client.open_by_url(LINK_EXCEL_NUBE)
        return ss.worksheet(name)
    except Exception as e:
        st.warning(f"⚠ No se pudo abrir hoja '{name}' en Sheets: {e}")
        return None

def fmt_num(x, dec=2):
    if x is None or x == "":
        return ""
    try:
        x = float(x)
    except:
        return x
    if float(x).is_integer():
        return int(x)
    return round(x, dec)

def enviar_filas_a_sheets(filas_sheets):
    if not filas_sheets:
        return
    ws = obtener_worksheet_por_nombre(HOJA_REGISTROS)
    if not ws:
        return
    try:
        ws.append_rows(filas_sheets, value_input_option="USER_ENTERED")
    except Exception as e:
        st.warning(f"⚠ No se pudo respaldar en Google Sheets: {e}")

# ------------------ SIDEBAR: LOGIN ADMIN ------------------
with st.sidebar:
    st.header("🔐 Admin")
    pwd = st.text_input("Contraseña", type="password")
    admin_activo = (pwd == PASSWORD_ADMIN and PASSWORD_ADMIN != "")
    if admin_activo:
        st.success("Admin activo ✅")
    else:
        st.info("Ingresa la contraseña para opciones de Admin.")

# ------------------ ADMIN: SOLO ABRIR GOOGLE SHEETS ------------------
if admin_activo:
    st.title("Panel de Administración")
    st.markdown(
        f'''
        <a href="{LINK_EXCEL_NUBE}" target="_blank">
            <button class="admin-button">📄 Abrir Google Sheets</button>
        </a>
        ''',
        unsafe_allow_html=True
    )
    st.stop()

# ------------------ UI PRINCIPAL ------------------
st.title("CONSUMOS Y RENDIMIENTOS 📈")

df_catalogo = cargar_catalogo_desde_db()
if df_catalogo.empty:
    st.warning("Catálogo vacío en TiDB.")
    st.stop()

# ------------------ REGIÓN FIJA DESDE LINK ------------------
region_fijada = st.query_params.get("region")

regiones = sorted(df_catalogo["Region"].dropna().unique().tolist())
if not region_fijada:
    st.error("Falta el parámetro de región en el link. Ejemplo: ?region=REGION_SUR")
    st.stop()

if region_fijada not in regiones:
    st.error(f"La región '{region_fijada}' no existe en tu catálogo.")
    st.stop()

selected_region = region_fijada
st.info(f"Región fijada automáticamente: **{selected_region}**")

# PLAZA + FECHA
c1, c2 = st.columns(2)
with c1:
    plazas = sorted(
        df_catalogo[df_catalogo["Region"] == selected_region]["Plaza"]
        .dropna().unique().tolist()
    )
    selected_plaza = st.selectbox("PLAZA", ["-- seleccionar --"] + plazas)

with c2:
    fecha = st.date_input("FECHA", value=date.today())
    if fecha > date.today():
        st.error("❌ No puedes capturar una fecha futura.")
        st.stop()

# PRECIOS
precio_magna = precio_premium = precio_gas = precio_diesel = 0.0
if selected_plaza != "-- seleccionar --":
    p1, p2, p3, p4 = st.columns(4)
    with p1:
        precio_gas = st.number_input("Precio Gas ($)", min_value=0.0, step=0.01, value=0.0)
    with p2:
        precio_magna = st.number_input("Precio Gasolina Magna ($)", min_value=0.0, step=0.01, value=0.0)
    with p3:
        precio_premium = st.number_input("Precio Gasolina Premium ($)", min_value=0.0, step=0.01, value=0.0)
    with p4:
        precio_diesel = st.number_input("Precio Diesel ($)", min_value=0.0, step=0.01, value=0.0)

st.divider()

# ------------------ TABLA DE CAPTURA ------------------
if selected_plaza != "-- seleccionar --":
    df_unidades = df_catalogo[
        (df_catalogo["Region"] == selected_region) &
        (df_catalogo["Plaza"] == selected_plaza)
    ].copy()

    ultimos_kms = obtener_ultimo_km_por_unidad_db()
    limites_dict = cargar_limites_rendimiento()

    rows = []
    for _, r in df_unidades.iterrows():
        unidad = str(r["Unidad"]).strip()
        if not unidad:
            continue

        km_base = r.get("Km inicial", 0) or 0
        km_ini = ultimos_kms.get(unidad, km_base)

        rows.append({
            "Unidad": unidad,
            "Km Final": None,
            "Gas (L)": 0.0,
            "Gasolina Magna (L)": 0.0,
            "Gasolina Premium (L)": 0.0,
            "Diesel (L)": 0.0,
            "_Modelo": r.get("Modelo", ""),
            "_Tipo": r.get("Tipo", ""),
            "_KM_INI": km_ini
        })

    df_editor = pd.DataFrame(rows)

    edited = st.data_editor(
        df_editor,
        hide_index=True,
        use_container_width=True,
        height=520,
        column_order=[
            "Unidad", "Km Final",
            "Gas (L)", "Gasolina Magna (L)",
            "Gasolina Premium (L)", "Diesel (L)"
        ]
    )

    if st.button("✅ GUARDAR"):
        filas_db = []
        filas_sheets = []
        errores = []
        hora = datetime.now().strftime("%H:%M:%S")

        historicos = obtener_ultimo_km_por_unidad_db()

        for _, row in edited.iterrows():
            unidad = row["Unidad"]
            if not unidad:
                continue

            km_ini = historicos.get(unidad, row["_KM_INI"] or 0)
            km_final = row["Km Final"]

            if km_final is None or str(km_final).strip() == "":
                continue

            try:
                km_final = float(km_final)
                km_ini = float(km_ini)
            except:
                errores.append(f"{unidad}: km inválido.")
                continue

            if km_final <= km_ini:
                errores.append(f"{unidad}: Km final ({km_final}) no puede ser <= Km inicial ({km_ini}).")
                continue

            km_rec = km_final - km_ini
            if km_rec > 1400:
                errores.append(f"{unidad}: Recorrido {km_rec:.0f} km mayor a 1400 km.")
                continue

            gas_l = float(row["Gas (L)"] or 0)
            magna_l = float(row["Gasolina Magna (L)"] or 0)
            premium_l = float(row["Gasolina Premium (L)"] or 0)
            diesel_l = float(row["Diesel (L)"] or 0)

            total_litros = gas_l + magna_l + premium_l + diesel_l
            if total_litros <= 0:
                errores.append(f"{unidad}: sin litros capturados.")
                continue

            rendimiento = km_rec / total_litros

            tipo = str(row["_Tipo"] or "").strip()
            modelo = str(row["_Modelo"] or "").strip()

            gas_p = gas_l * precio_gas
            g_magna_p = magna_l * precio_magna
            g_premium_p = premium_l * precio_premium
            diesel_p = diesel_l * precio_diesel
            total_importe = gas_p + g_magna_p + g_premium_p + diesel_p

            key = (str(selected_region).strip(), tipo, modelo)
            lim_inf = lim_sup = None
            if key in limites_dict:
                lim_inf, lim_sup = limites_dict[key]

            filas_db.append((
                fecha.strftime("%Y-%m-%d"),
                selected_region,
                selected_plaza,
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
                total_litros, total_importe,
                rendimiento, lim_sup, lim_inf,
                hora
            ))

            filas_sheets.append([
                fecha.strftime("%Y-%m-%d"),
                selected_region,
                selected_plaza,
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
        else:
            if not filas_db:
                st.warning("No hay filas válidas para guardar.")
            else:
                try:
                    insertar_registros_diarios(filas_db)
                    st.success("✅ Guardado en TiDB.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ Error guardando en TiDB: {e}")
                    st.stop()

                enviar_filas_a_sheets(filas_sheets)
                st.balloons()
                st.rerun()


