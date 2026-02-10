import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date, datetime
import gspread
from google.oauth2.service_account import Credentials
import json
import pytz

def safe_float(valor):
    if valor is None:
        return 0.0
    try:
        return float(valor)
    except:
        return 0.0

# ================== SESSION STATE ==================
if "guardado_ok" not in st.session_state:
    st.session_state.guardado_ok = False
if "sheets_error" not in st.session_state:
    st.session_state.sheets_error = None

if "modo" not in st.session_state:
    st.session_state.modo = "normal"

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
PASSWORD_EDITOR = "edit123"

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
        
        for fila in filas:
            try:
                fila_limpia = [clean_for_sheets(v) for v in fila]
                ws.append_row(fila_limpia, value_input_option="USER_ENTERED")
            except:
                continue
    except Exception as e:
        st.session_state.sheets_error = f"Sheets Falló: {e}"

# ================== UI ==================
tz_mzt = pytz.timezone('America/Mazatlan')
fecha_hoy_mzt = datetime.now(tz_mzt).date()

# ================== BARRA LATERAL (CONTROL DE ACCESO) ==================
with st.sidebar:
    st.header("🔐 Acceso")
    password = st.text_input("Contraseña", type="password")

    # Lógica de niveles de acceso
    if password == PASSWORD_ADMIN:
        st.session_state.modo = "admin"
    elif password == PASSWORD_EDITOR:
        st.session_state.modo = "editor"
    else:
        st.session_state.modo = "normal"

    # Si hay una sesión activa (Admin o Editor)
    if st.session_state.modo != "normal":
        st.divider()
        st.write(f"Sesión: **{st.session_state.modo.upper()}**")

        # BOTÓN VERDE DE SHEETS (Solo si es Admin)
        if st.session_state.modo == "admin":
            st.markdown(f"""
                <a href="https://docs.google.com/spreadsheets/d/1BHrjyuJcRhof5hp5VzjoGDzbB6i7olcp2mH8DkF3LwE/edit?gid=0#gid=0" target="_blank" style="text-decoration: none;">
                    <div class="admin-button" style="text-align: center; margin-top: 10px; margin-bottom: 10px;">
                        🟢 GOOGLE SHEETS
                    </div>
                </a>
            """, unsafe_allow_html=True)

# ================== PANTALLA EDITOR ==================
if st.session_state.modo == "editor":
    st.title("🛠️ Editor de registros")
    
    # -------- FILTROS ----------
    c1, c2, c3 = st.columns(3)

    with c1:
        fecha_busqueda = st.date_input("Fecha", fecha_hoy_mzt)

    with c2:
        df_cat = cargar_catalogo()
        plazas = ["Seleccione"] + sorted(df_cat["Plaza"].dropna().unique())
        plaza_busqueda = st.selectbox("Plaza", plazas)

    with c3:
        unidad_busqueda = st.selectbox(
            "Unidad",
            ["Seleccione"] + sorted(df_cat[df_cat["Plaza"] == plaza_busqueda]["Unidad"].unique())
        )

    # -------- QUERY ----------
    query = f"""
        SELECT *
        FROM registro_diario
        WHERE fecha = '{fecha_busqueda}'
        AND plaza = '{plaza_busqueda}'
    """

    if unidad_busqueda != "Seleccione":
        query += f" AND unidad = '{unidad_busqueda}'"

    df_para_editar = run_select(query)

    if df_para_editar.empty:
        st.warning("No se encontraron registros.")
        st.stop()

    # -------- EDITOR ----------
    columnas_editables = [
        "km_inicial", "km_final",
        "gas_l", "g_magna_l", "g_premium_l", "diesel_l"
    ]

    df_editado = st.data_editor(
        df_para_editar,
        hide_index=True,
        disabled=[c for c in df_para_editar.columns if c not in columnas_editables],
        use_container_width=True
    )

    # -------- GUARDAR ----------
    if st.button("💾 Guardar cambios"):
        try:
            conn = get_connection()
            cur = conn.cursor()

            for _, r in df_editado.iterrows():

                # cálculos nuevos
                km_rec = safe_float(r["km_final"]) - safe_float(r["km_inicial"])

                total_litros = (
                    safe_float(r["gas_l"]) +
                    safe_float(r["g_magna_l"]) +
                    safe_float(r["g_premium_l"]) +
                    safe_float(r["diesel_l"])
                )

                total_importe = (
                    safe_float(r["gas_l"]) * safe_float(r["gas_p"]) +
                    safe_float(r["g_magna_l"]) * safe_float(r["g_magna_p"]) +
                    safe_float(r["g_premium_l"]) * safe_float(r["g_premium_p"]) +
                    safe_float(r["diesel_l"]) * safe_float(r["diesel_p"])
                )

                rendimiento = km_rec / total_litros if total_litros > 0 else 0

                cur.execute("""
                    UPDATE registro_diario
                    SET km_inicial=%s,
                        km_final=%s,
                        km_recorridos=%s,
                        gas_l=%s,
                        g_magna_l=%s,
                        g_premium_l=%s,
                        diesel_l=%s,
                        total_litros=%s,
                        total_importe=%s,
                        rendimiento_real=%s
                    WHERE id=%s
                """, (
                    r["km_inicial"],
                    r["km_final"],
                    km_rec,
                    r["gas_l"],
                    r["g_magna_l"],
                    r["g_premium_l"],
                    r["diesel_l"],
                    total_litros,
                    total_importe,
                    rendimiento,
                    r["id"]
                ))

            conn.commit()
            cur.close()
            conn.close()

            ultimo_km.clear()
            st.success("✅ Registros actualizados")

        except Exception as e:
            st.error(f"❌ error al actualizar: {e}")

    if st.button("⬅ volver"):
        st.session_state.modo = "normal"
        st.rerun()

    st.stop()

# ================== MENU PRINCIPAL ==================
pestana = st.sidebar.radio(
    "Menú",
    ["Captura", "Registros"]
)

# si eligen registros, mostramos consulta y detenemos captura
if pestana == "Registros":
    st.title("📋 Consulta de registros")

    c1, c2, c3 = st.columns(3)

    with c1:
        fecha_filtro = st.date_input("Fecha", fecha_hoy_mzt)

    with c2:
        df_cat = cargar_catalogo()
        plazas = ["TODAS"] + sorted(df_cat["Plaza"].dropna().unique())
        plaza_filtro = st.selectbox("Plaza", plazas)

    with c3:
        if plaza_filtro == "TODAS":
            unidades = ["TODAS"]
        else:
            unidades = ["TODAS"] + sorted(
                df_cat[df_cat["Plaza"] == plaza_filtro]["Unidad"].unique()
            )

        unidad_filtro = st.selectbox("Unidad", unidades)

    # -------- QUERY ----------
    query = f"""
        SELECT *
        FROM registro_diario
        WHERE fecha = '{fecha_filtro}'
    """

    if plaza_filtro != "TODAS":
        query += f" AND plaza = '{plaza_filtro}'"

    if unidad_filtro != "TODAS":
        query += f" AND unidad = '{unidad_filtro}'"

    query += " ORDER BY plaza, unidad"

    df_registros = run_select(query)

    if df_registros.empty:
        st.warning("No hay información.")
    else:
        st.dataframe(df_registros, use_container_width=True)

    st.stop()

st.title("CONSUMOS Y RENDIMIENTOS 📈")
    
if st.session_state.guardado_ok:
    st.success("✅ Guardado correctamente en la base de datos.")

    if st.session_state.get("sheets_error"):
        st.warning(
            f"⚠️ Atención: TiDB guardó, pero la sincronización con Sheets falló: {st.session_state.sheets_error}"
        )
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
with c3: fecha = st.date_input("FECHA", fecha_hoy_mzt, max_value=fecha_hoy_mzt)

if ya_hay_captura(region, plaza, fecha):
    st.markdown("---")
    st.info("🌟 **Gracias por capturar el día de hoy, nos vemos mañana.**")
    
    # --- LÓGICA DE UNIDADES FALTANTES ---
    # 1. Obtenemos todas las unidades que DEBERÍAN estar (Lista Maestra)
    unidades_esperadas = set(df[(df.Region == region) & (df.Plaza == plaza)]["Unidad"].unique())
    
    # 2. Consultamos qué unidades YA se capturaron hoy en la DB
    query_hoy = f"""
        SELECT DISTINCT unidad 
        FROM registro_diario 
        WHERE region = '{region}' AND plaza = '{plaza}' AND fecha = '{fecha}'
    """
    df_capturadas_hoy = run_select(query_hoy)
    unidades_capturadas = set(df_capturadas_hoy["unidad"].unique())
    
    # 3. Calculamos la diferencia
    faltantes = sorted(list(unidades_esperadas - unidades_capturadas))
    
    # 4. Mostramos el reporte
    if faltantes:
        st.warning(f"⚠️ **Atención:** Faltaron de capturar {len(faltantes)} unidades:")
        # Las mostramos en columnas pequeñas para que no ocupen mucho espacio
        cols = st.columns(5)
        for i, unidad_f in enumerate(faltantes):
            cols[i % 5].write(f"• {unidad_f}")
    else:
        st.success("✅ **¡Excelente!** Se capturaron todas las unidades de esta plaza.")
    
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
    filtered_df = filtered_df.drop(columns=['Unidad_Num'])
except:
    filtered_df = filtered_df.sort_values(by='Unidad', ascending=True)

rows = []
for _, r in filtered_df.iterrows():
    unidad = str(r.Unidad)
    km_previo = kms.get(unidad) 
    km_ini = km_previo if km_previo is not None and km_previo > 0 else float(r["Km inicial"] or 0)
    
    rows.append({
        "Unidad": unidad, 
        "Km Final": None, 
        "Gas (L)": 0.0, 
        "Magna (L)": 0.0, 
        "Premium (L)": 0.0, 
        "Diesel (L)": 0.0,
        "_km_ini": km_ini, 
        "_tipo": r.Tipo, 
        "_modelo": r.Modelo, 
        "_lim_sup": r.lim_sup, 
        "_lim_inf": r.lim_inf
    })

ed = st.data_editor(
    pd.DataFrame(rows), 
    hide_index=True, 
    column_config={
        "Unidad": st.column_config.TextColumn("Unidad", disabled=True),
        "Km Final": st.column_config.NumberColumn("Km Final", min_value=0, step=1, format="%d"),
        "Gas (L)": st.column_config.NumberColumn("Gas (L)", min_value=0.0, format="%.2f"),
        "Magna (L)": st.column_config.NumberColumn("Magna (L)", min_value=0.0, format="%.2f"),
        "Premium (L)": st.column_config.NumberColumn("Premium (L)", min_value=0.0, format="%.2f"),
        "Diesel (L)": st.column_config.NumberColumn("Diesel (L)", min_value=0.0, format="%.2f"),
        "_km_ini": None, "_tipo": None, "_modelo": None, "_lim_sup": None, "_lim_inf": None
    }
)
table_messages = st.container()

# ================== GUARDAR CORREGIDO ==================
if st.button("GUARDAR✅"):
    if precio_gas <= 0 or precio_magna <= 0 or precio_premium <= 0 or precio_diesel <= 0:
        table_messages.error("❌ ERROR: Debe ingresar los precios de TODOS los combustibles.")
        st.stop()

    filas_db, filas_sh = [], []
    ahora_mzt = datetime.now(tz_mzt)
    hora_mx = ahora_mzt.strftime("%H:%M:%S")
    has_critical_error = False
    
    for index, x in ed.iterrows():
        if x["Km Final"] is None: 
            continue
            
        try:
           km_f = safe_float(x["Km Final"])
           km_i = safe_float(x["_km_ini"])

        except:
            continue

        # Litros por tipo
        g = safe_float(x["Gas (L)"])
        m = safe_float(x["Magna (L)"])
        p = safe_float(x["Premium (L)"])
        d = safe_float(x["Diesel (L)"])

        
        # TOTALES (Aquí estaba el fallo)
        total_litros = g + m + p + d
        total_importe = (g*precio_gas + m*precio_magna + p*precio_premium + d*precio_diesel)
        kmr = km_f - km_i

        if total_litros <= 0 and km_f == km_i: 
            continue
        
        if total_litros <= 0 and km_f != km_i:
            table_messages.error(f"❌ {x['Unidad']}: Falta capturar litros.")
            has_critical_error = True
            break

        if km_f < km_i:
            table_messages.error(f"❌ {x['Unidad']}: El Km Final no puede ser menor al Inicial.")
            has_critical_error = True
            break

        if kmr > 1900:
            table_messages.error(f"❌ {x['Unidad']}: El recorrido ({kmr} km) es demasiado alto. Máximo 1900 km.")
            has_critical_error = True
            break

        rend = kmr / total_litros if total_litros > 0 else 0

        # FILA COMPLETA CON 23 COLUMNAS EN ORDEN
        fila = (
            fecha, region, plaza, x["Unidad"], x["_tipo"], x["_modelo"], 
            km_i, km_f, kmr,
            g, g*precio_gas,      # Gas L y $
            m, m*precio_magna,    # Magna L y $
            p, p*precio_premium,  # Premium L y $
            d, d*precio_diesel,   # Diesel L y $
            total_litros,         # <--- Columna total_litros (18)
            total_importe,        # <--- Columna total_importe (19)
            rend,                 # <--- Columna rendimiento_real (20)
            x["_lim_sup"] if x["_lim_sup"] > 0 else None, 
            x["_lim_inf"] if x["_lim_inf"] > 0 else None, 
            hora_mx
        )

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
            table_messages.error(f"❌ Error al guardar en TiDB: {e}")














































































