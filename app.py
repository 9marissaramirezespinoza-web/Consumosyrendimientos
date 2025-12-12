import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date, datetime
import gspread
from google.oauth2.service_account import Credentials

# ------------------ CONFIG DB ------------------
DB_HOST = "gateway01.ap-northeast-1.prod.aws.tidbcloud.com"
DB_PORT = 4000
DB_USER = "21dAYBizAVcha72.root"
DB_PASSWORD = "PJwovbR7In1xYCj0"
DB_NAME = "consumos"
DB_SSL_CA = "isrgrootx1.pem"  # Certificado para TiDB Cloud

# ------------------ CONFIG GOOGLE SHEETS ------------------
LINK_EXCEL_NUBE = "https://docs.google.com/spreadsheets/d/1BHrjyuJcRhof5hp5VzjoGDzbB6i7olcp2mH8DkF3LwE/edit?hl=es&gid=0#gid=0"
ARCHIVO_LLAVE = "credentials.json"
HOJA_REGISTROS = "REGISTROS"
PASSWORD_ADMIN = "tec123"

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

# ------------------ DB HELPERS (TiDB) ------------------
def get_connection():
    """Crea y devuelve una conexión a TiDB."""
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        ssl_ca=DB_SSL_CA
    )
    return conn


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
    """
    Carga catálogo de unidades desde TiDB.
    Renombra columnas para usarlas en la UI.
    """
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
    """
    Devuelve dict {unidad: último_km_final} desde registro_diario.
    Sirve para el 'km inicial fantasma'.
    """
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
    """
    Carga tabla limites_rendimiento y regresa un dict:
    {(region,tipo,modelo): (lim_inf, lim_sup)}
    """
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
    """
    Inserta múltiples registros en registro_diario en TiDB.
    """
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
def get_gspread_client():
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(ARCHIVO_LLAVE, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.warning(f"⚠ No se pudo inicializar Google Sheets ({e}). No se respaldará en Sheets.")
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
    """Redondea número para que no tenga un chingo de ceros."""
    if x is None or x == "":
        return ""
    try:
        x = float(x)
    except:
        return x
    if x.is_integer():
        return int(x)
    return round(x, dec)


def enviar_filas_a_sheets(filas_sheets):
    """
    Agrega filas al Google Sheets (solo append, sin borrar ni formatear).
    """
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
    admin_activo = (pwd == PASSWORD_ADMIN)
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

    st.stop()  # No mostrar nada más si es admin


# ------------------ UI PRINCIPAL: CAPTURA ------------------
st.title("CONSUMOS Y RENDIMIENTOS 📈")

df_catalogo = cargar_catalogo_desde_db()
if df_catalogo.empty:
    st.warning("Catálogo vacío en TiDB.")
    st.stop()

# REGION / PLAZA / FECHA (fecha abierta, pero sin futuro)
c1, c2, c3 = st.columns(3)
with c1:
    regiones = sorted(df_catalogo['Region'].dropna().unique().tolist())
# ===============================
#   FIJAR REGIÓN DESDE EL LINK
# ===============================

# Leer parámetro "region" desde la URL usando la API nueva
region_fijada = st.query_params.get("region")

# Lista de regiones del catálogo
regiones = sorted(df_catalogo['Region'].dropna().unique().tolist())

# Si viene región en el link y es válida:
if region_fijada and region_fijada in regiones:

    selected_region = region_fijada
    
    # Mostrar mensaje para que el usuario sepa que viene fijada en el link
    st.info(f"Región fijada automáticamente: **{selected_region}**")

else:
    # Si no viene región en el link, permitir seleccionar
    selected_region = st.selectbox("REGIÓN", ["-- seleccionar --"] + regiones)


with c2:
    if selected_region != "-- seleccionar --":
        plazas = sorted(
            df_catalogo[df_catalogo['Region'] == selected_region]['Plaza']
            .dropna()
            .unique()
            .tolist()
        )
    else:
        plazas = []
    selected_plaza = st.selectbox("PLAZA", ["-- seleccionar --"] + plazas)
with c3:
    fecha = st.date_input("FECHA", value=date.today())
    if fecha > date.today():
        st.error("❌ No puedes capturar una fecha futura.")
        st.stop()

# Precios (solo si ya eligieron región y plaza)
precio_magna = 0.0
precio_premium = 0.0
precio_gas = 0.0
precio_diesel = 0.0

if selected_region != "-- seleccionar --" and selected_plaza != "-- seleccionar --":
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
            "_Año": r.get("Año", ""),
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
            "Unidad",
            "Km Final",
            "Gas (L)",
            "Gasolina Magna (L)",
            "Gasolina Premium (L)",
            "Diesel (L)"
        ]
    )

    if st.button("✅ GUARDAR"):
        filas_db = []
        filas_sheets = []
        errores = []
        hora = datetime.now().strftime("%H:%M:%S")  # Solo hora

        historicos = obtener_ultimo_km_por_unidad_db()

        for idx, row in edited.iterrows():
            unidad = row["Unidad"]
            if not unidad:
                continue

            km_ini = historicos.get(unidad, row["_KM_INI"] or 0)
            km_final = row["Km Final"]

            if km_final is None or str(km_final).strip() == "":
                continue  # no guardar si no puso km final

            try:
                km_final = float(km_final)
                km_ini = float(km_ini)
            except:
                errores.append(f"{unidad}: km inválido.")
                continue

            if km_final <= km_ini:
                errores.append(
                    f"{unidad}: Km final ({km_final}) no puede ser menor o igual que Km inicial ({km_ini})."
                )
                continue

            km_rec = km_final - km_ini

            # Alerta si > 1400
            if km_rec > 1400:
                errores.append(
                    f"{unidad}: Recorrido {km_rec:.0f} km mayor a 1400 km. Verifique el kilometraje."
                )
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

            # Importes con los precios capturados
            gas_p = gas_l * precio_gas
            g_magna_p = magna_l * precio_magna
            g_premium_p = premium_l * precio_premium
            diesel_p = diesel_l * precio_diesel

            total_importe = gas_p + g_magna_p + g_premium_p + diesel_p

            # Límites de rendimiento
            key = (str(selected_region).strip(), tipo, modelo)
            lim_inf = None
            lim_sup = None
            if key in limites_dict:
                lim_inf, lim_sup = limites_dict[key]

            # Tupla para DB
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

            # Fila para Google Sheets (sin columna ID)
            filas_sheets.append([
                fecha.strftime("%Y-%m-%d"),          # FECHA
                selected_region,                     # REGION
                selected_plaza,                      # PLAZA
                unidad,                              # UNIDAD
                tipo,                                # TIPO
                modelo,                              # MODELO
                fmt_num(km_ini, 0),                  # KM INICIAL
                fmt_num(km_final, 0),                # KM FINAL
                fmt_num(km_rec, 0),                  # KM RECORRIDOS
                fmt_num(gas_l, 2),                   # GAS (L)
                fmt_num(gas_p, 2),                   # GAS ($)
                fmt_num(magna_l, 2),                 # G MAGNA (L)
                fmt_num(g_magna_p, 2),               # G MAGNA ($)
                fmt_num(premium_l, 2),               # G PREMIUM (L)
                fmt_num(g_premium_p, 2),             # G PREMIUM ($)
                fmt_num(diesel_l, 2),                # DIESEL (L)
                fmt_num(diesel_p, 2),                # DIESEL ($)
                fmt_num(total_litros, 3),            # TOTAL LITROS
                fmt_num(total_importe, 2),           # TOTAL IMPORTE
                fmt_num(rendimiento, 3),             # RENDIMIENTO REAL
                fmt_num(lim_sup, 3) if lim_sup is not None else "",  # LIMITE SUPERIOR
                fmt_num(lim_inf, 3) if lim_inf is not None else "",  # LIMITE INFERIOR
                hora                                 # HORA REGISTRO
            ])

        if errores:
            for e in errores:
                st.error(e)
        else:
            if not filas_db:
                st.warning("No hay filas válidas para guardar.")
            else:
                # 1) Guardar en TiDB
                try:
                    insertar_registros_diarios(filas_db)
                    st.success("✅ Registros guardados correctamente en TiDB.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ Error guardando en TiDB: {e}")
                    st.stop()

                # 2) Respaldo en Google Sheets (best effort)
                try:
                    enviar_filas_a_sheets(filas_sheets)
                except Exception as e:
                    st.warning(f"⚠ No se pudo respaldar en Google Sheets: {e}")

                st.balloons()
                st.rerun()

