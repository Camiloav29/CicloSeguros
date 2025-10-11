from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, send_file
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import os
import pandas as pd
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import uuid
import config_manager
from admin.routes import admin_bp

def limpiar_valor_moneda(valor_str):
    """
    Limpia un string de moneda (ej. '$1.500.000' o '1.500,50') a un float.
    Esta versión es robusta y maneja puntos como separadores de miles y comas como decimales.
    """
    if isinstance(valor_str, (int, float)):
        return float(valor_str)
    if not isinstance(valor_str, str):
        return 0.0

    # 1. Quitar símbolos de moneda, espacios y separadores de miles (puntos)
    valor_limpio = valor_str.replace('$', '').replace('.', '').strip()

    # 2. Reemplazar la coma decimal por un punto para la conversión a float
    valor_limpio = valor_limpio.replace(',', '.')

    if not valor_limpio:
        return 0.0
    try:
        return float(valor_limpio)
    except (ValueError, TypeError):
        return 0.0

def get_year_from_date(date_str):
    """
    Extracts the year from a date string.
    Tries to parse 'YYYY-MM-DD' and 'dd/mm/YYYY' formats.
    Returns the year as a string or None if parsing fails.
    """
    if not date_str or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()

    # Try parsing YYYY-MM-DD first, as reported by the user
    try:
        return str(datetime.strptime(date_str, '%Y-%m-%d').year)
    except ValueError:
        # If that fails, try the original dd/mm/YYYY format
        try:
            return str(datetime.strptime(date_str, '%d/%m/%Y').year)
        except (ValueError, TypeError):
            return None

app = Flask(__name__) # Ensure app instance is created
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'dev_super_secret_key_12345_replace_in_production')

# --- Login Manager Configuration ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # The name of the view to redirect to when login is required.
login_manager.login_message = "Por favor, inicie sesión para acceder a esta página."
login_manager.login_message_category = "info"

# --- User Model and In-Memory Store ---
class User(UserMixin):
    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

# In-memory user database
users = {
    "1": User(id="1", username="admin", password_hash=generate_password_hash("1234"))
}

@login_manager.user_loader
def load_user(user_id):
    return users.get(user_id)

# Rutas BASE_DIR debe estar al nivel de donde corre app.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'archivos_subidos')
CONSECUTIVO_FILE = os.path.join(BASE_DIR, 'consecutivo.txt')
EXCEL_FILE = os.path.join(BASE_DIR, 'remisiones.xlsx')
CLIENT_FOLDERS_BASE_DIR = os.path.join(BASE_DIR, 'CLIENTES_CARPETAS')
VENDEDOR_FOLDERS_BASE_DIR = os.path.join(BASE_DIR, 'VENDEDORES_CARPETAS')
CARTERA_DATA_DIR_NAME = 'DATOS_CARTERA' # Folder name
CARTERA_DATA_DIR = os.path.join(BASE_DIR, CARTERA_DATA_DIR_NAME)
CARTERA_PROCESADA_FILENAME = 'cartera_procesada.xlsx'
# Define column name constants (using names exactly as they appear in the uploaded Excel for extraction)
COLUMNAS_A_EXTRAER_CARTERA = ['NÚMERO PÓLIZA', 'ASEGURADORA', 'NOMBRES CLIENTE', 'PRIMA NETA', 'COMISIÓN', 'PORCENTAJE DE COMISIÓN', 'FECHA CREACIÓN', 'VENDEDOR']
COLUMNAS_CALCULADAS_CARTERA = [
    'Retencion_Calc',
    'Reteica_Calc',
    'Valor_Comision_UIB_Neto_Calc',
    'Intermediario_Original',
    'Porc_Com_Intermediario_Original',
    'Valor_Comision_Intermediario_Calc'
]
COLUMNAS_MANUALES_CARTERA = [
    'Clasificacion_Manual',
    'Line_of_Business_Manual',
    'N_FACTURA_Manual'
]
NUEVAS_COLUMNAS_CARTERA = COLUMNAS_CALCULADAS_CARTERA + COLUMNAS_MANUALES_CARTERA

ORDEN_COLUMNAS_EXCEL_CARTERA = [
    'ID_CARTERA', 'FECHA CREACIÓN', 'N_FACTURA_Manual', 'NÚMERO PÓLIZA',
    'ASEGURADORA', 'NOMBRES CLIENTE', 'PRIMA NETA', 'COMISIÓN',
    'PORCENTAJE DE COMISIÓN', 'VENDEDOR',
    'Retencion_Calc', 'Reteica_Calc', 'Valor_Comision_UIB_Neto_Calc',
    'Intermediario_Original', # This is created from VENDEDOR
    'Porc_Com_Intermediario_Original', # This is created from PORCENTAJE DE COMISIÓN
    'Valor_Comision_Intermediario_Calc',
    'Clasificacion_Manual', 'Line_of_Business_Manual'
]

# --- Vencimientos Module Constants & Config ---
VENCIMIENTOS_DATA_DIR_NAME = 'DATOS_VENCIMIENTOS'
VENCIMIENTOS_DATA_DIR = os.path.join(BASE_DIR, VENCIMIENTOS_DATA_DIR_NAME)
VENCIMIENTOS_PROCESADOS_FILENAME = 'vencimientos_procesados.xlsx'

# --- Prospectos Module Constants & Config ---
PROSPECTOS_DATA_DIR_NAME = 'DATOS_PROSPECTOS'
PROSPECTOS_DATA_DIR = os.path.join(BASE_DIR, PROSPECTOS_DATA_DIR_NAME)
PROSPECTOS_FILENAME = 'prospectos.xlsx'

OPCIONES_RESPONSABLE_TECNICO = ['Luisa', 'Valentina', 'Jairo', 'Jose', 'William']
OPCIONES_RESPONSABLE_COMERCIAL = ['Jose', 'Valentina', 'Jairo', 'Pedro', 'William', 'Camila']
OPCIONES_ESTADO_PROSPECTO = ['Activo', 'En gestión', 'Cotización', 'Pdte respuesta', 'Ganado', 'Perdido']

# Define the final column order for the prospectos.xlsx file
ORDEN_COLUMNAS_PROSPECTOS = [
    'ID_PROSPECTO',
    'Nombre Cliente',
    'Responsable Tecnico',
    'Responsable Comercial',
    'Fecha de Cotizacion',
    'Fecha inicio poliza',
    'es_TPP',
    'Nombre_TPP',
    'Porcentaje_comision_TPP',
    'Ramo',
    'Aseguradora',
    'Prima',
    'Comision %',
    'Comision $',
    'Estado',
    'Observaciones',
    'Fecha Creacion'
]

COLUMNAS_A_EXTRAER_VENCIMIENTOS = ['FECHA FIN', 'NÚMERO PÓLIZA', 'NOMBRES CLIENTE', 'ASEGURADORA', 'RAMO PRINCIPAL']

COLUMNAS_ADICIONALES_VENCIMIENTOS = [
    'Fecha_inicio_seguimiento',
    'Responsable',
    'Estado',
    'Observaciones_adicionales',
    'Remision_Asociada' # Nueva columna
]



ORDEN_COLUMNAS_VENCIMIENTOS = [
    'ID_VENCIMIENTO', 'FECHA FIN', 'Fecha_inicio_seguimiento',
    'NÚMERO PÓLIZA', 'NOMBRES CLIENTE', 'ASEGURADORA', 'RAMO PRINCIPAL',
    'Responsable', 'Estado', 'Observaciones_adicionales',
    'Remision_Asociada' # Nueva columna
]

# --- Cobros Module Constants & Config ---
COBROS_FILENAME = 'cobros.xlsx'
COBROS_FILE = os.path.join(BASE_DIR, COBROS_FILENAME)
ORDEN_COLUMNAS_COBROS = [
    'ID_COBRO', 'CONSECUTIVO_REMISION', 'Tomador', 'NIT_CC', 'Aseguradora', 'Ramo',
    'N_Poliza', 'N_Cuota', 'Total_Cuotas', 'Fecha_Vencimiento_Cuota',
    'Fecha_Inicio_Vigencia', 'Fecha_Fin_Vigencia', 'Estado', 'Tipo_Movimiento',
    'Periodicidad'
]

# This is the definitive column order for remisiones.xlsx
# It includes all fields from the form, including calculated ones.
ORDEN_COLUMNAS_EXCEL_REMISIONES = [
    'consecutivo', 'estado', 'fecha_registro', # Automatic fields
    # Checkboxes
    'renovacion', 'negocio_nuevo', 'renovable', 'modificacion', 'anexo_checkbox', 'policy_number_modified',
    # Datos Básicos
    'fecha_recepcion', 'tomador', 'nit', 'aseguradora', 'ramo', 'poliza', 'old_policy_number', 'anexo',
    'categorias_grupo', 'categorias_grupo_otro', # Handling for "Otro"
    'fecha_inicio', 'fecha_fin', 'fecha_limite_pago',

    # Información de Venta y Comisiones
    'tipo_moneda', 'prima_neta', 'porcentaje_comision_valor',
    'Comision$', # Calculated
    'vendedor', 'porcentaje_vendedor',
    'co_corretaje_opcion', 'co_corretaje_nombre', 'co_corretaje_porcentaje',
    'ComisionTPP', # Calculated
    'ComisionUIB', # Calculated
    'uib', # This is the final value after all calculations, should match ComisionUIB
    'gastos_adicionales',

    # Condiciones y Vigencia
    'forma_pago', 'numero_cuotas', 'periodicidad_pago',

    # Observaciones
    'observaciones', 'riesgos_adicionales', 'analista_responsable',

    # System fields
    'archivos',
    'numero_remision_manual'
]

# Configurar carpeta de carga
os.makedirs(UPLOAD_FOLDER, exist_ok=True) # For remision attachments
os.makedirs(CLIENT_FOLDERS_BASE_DIR, exist_ok=True) # For client folders
os.makedirs(VENDEDOR_FOLDERS_BASE_DIR, exist_ok=True) # For vendor folders
os.makedirs(CARTERA_DATA_DIR, exist_ok=True) # For cartera data
os.makedirs(VENCIMIENTOS_DATA_DIR, exist_ok=True) # For vencimientos data
os.makedirs(PROSPECTOS_DATA_DIR, exist_ok=True) # For prospectos data
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['CLIENT_FOLDERS_BASE_DIR'] = CLIENT_FOLDERS_BASE_DIR
app.config['VENDEDOR_FOLDERS_BASE_DIR'] = VENDEDOR_FOLDERS_BASE_DIR
app.config['CARTERA_DATA_DIR'] = CARTERA_DATA_DIR
app.config['CARTERA_PROCESADA_FILE_PATH'] = os.path.join(CARTERA_DATA_DIR, CARTERA_PROCESADA_FILENAME)
app.config['VENCIMIENTOS_DATA_DIR'] = VENCIMIENTOS_DATA_DIR
app.config['VENCIMIENTOS_PROCESADA_FILE_PATH'] = os.path.join(VENCIMIENTOS_DATA_DIR, VENCIMIENTOS_PROCESADOS_FILENAME)
app.config['PROSPECTOS_DATA_DIR'] = PROSPECTOS_DATA_DIR
app.config['PROSPECTOS_FILE_PATH'] = os.path.join(PROSPECTOS_DATA_DIR, PROSPECTOS_FILENAME)

# Registrar el Blueprint de administración
app.register_blueprint(admin_bp)

# Obtener el consecutivo
def obtener_consecutivo():
    if not os.path.exists(CONSECUTIVO_FILE):
        with open(CONSECUTIVO_FILE, 'w') as f:
            f.write('1')
        num = 1
    else:
        with open(CONSECUTIVO_FILE, 'r') as f:
            try:
                num = int(f.read().strip())
            except ValueError:
                num = 1 # Default to 1 if file is empty or corrupt

    year_short = datetime.now().strftime('%y')
    consecutivo_actual = f"UIB-{year_short}-{num:05d}"

    nuevo_num = num + 1
    with open(CONSECUTIVO_FILE, 'w') as f:
        f.write(str(nuevo_num))

    return consecutivo_actual

def guardar_remision(datos):
    df = pd.DataFrame([datos])
    try:
        if os.path.exists(EXCEL_FILE):
            df_existente = pd.read_excel(EXCEL_FILE)
            df_final = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_final = df

        # ORDEN_COLUMNAS_EXCEL_REMISIONES should be the global list defined in a previous step.
        # Ensure all columns from the master order list exist in df_final.
        # Initialize missing columns with an empty string, assuming most are text or can be empty text.
        # More specific default values (like 0 for numbers, False for booleans) might be needed
        # if strict data types are required right at this stage for Excel, but usually empty strings are fine for Excel.
        if 'ORDEN_COLUMNAS_EXCEL_REMISIONES' in globals() and isinstance(ORDEN_COLUMNAS_EXCEL_REMISIONES, list):
            current_columns = df_final.columns.tolist()
            for col_maestra in ORDEN_COLUMNAS_EXCEL_REMISIONES:
                if col_maestra not in current_columns:
                    df_final[col_maestra] = ""  # Add missing column with empty strings

            # Reorder df_final according to the master list
            # Select only columns that are in ORDEN_COLUMNAS_EXCEL_REMISIONES to avoid KeyErrors if df_final has extra columns
            # And also ensure that all columns from ORDEN_COLUMNAS_EXCEL_REMISIONES are present (handled by loop above)
            df_final = df_final[ORDEN_COLUMNAS_EXCEL_REMISIONES]
        else:
            print("ADVERTENCIA: ORDEN_COLUMNAS_EXCEL_REMISIONES no está definida o no es una lista. remisiones.xlsx se guardará con el orden actual del DataFrame.")

        df_final.to_excel(EXCEL_FILE, index=False)
        return True
    except Exception as e:
        print(f"Error al guardar en Excel: {e}")
        return False

def guardar_cobros(nuevos_cobros):
    df = pd.DataFrame(nuevos_cobros)
    try:
        if os.path.exists(COBROS_FILE):
            df_existente = pd.read_excel(COBROS_FILE)
            df_final = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_final = df

        # Ensure all columns exist and are in order
        for col in ORDEN_COLUMNAS_COBROS:
            if col not in df_final.columns:
                df_final[col] = ""
        df_final = df_final[ORDEN_COLUMNAS_COBROS]

        df_final.to_excel(COBROS_FILE, index=False)
        return True
    except Exception as e:
        print(f"Error al guardar en {COBROS_FILENAME}: {e}")
        return False

def cargar_remisiones():
    if os.path.exists(EXCEL_FILE):
        try:
            return pd.read_excel(EXCEL_FILE).to_dict(orient='records')
        except Exception as e:
            print(f"Error al cargar desde Excel: {e}")
            return []
    return []

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        # Find user by username
        user = None
        for u in users.values():
            if u.username == username:
                user = u
                break
        
        if user and check_password_hash(user.password_hash, password):
            login_user(user)
            flash('Inicio de sesión exitoso.', 'success')
            return redirect(url_for('index'))
        else:
            flash('Nombre de usuario o contraseña incorrectos.', 'danger')
            
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Ha cerrado sesión exitosamente.', 'info')
    return redirect(url_for('login'))

@app.route('/', methods=['GET'])
@login_required
def index():
    # --- Initialize data structures ---
    kpis = {
        'vencimientos_15_dias': 0,
        'cobros_pendientes_mes': 0, 'prospectos_en_gestion': 0,
        'produccion_mes': 0, 'remisiones_pendientes': 0
    }
    produccion_por_ramo_chart = {'labels': [], 'data': []}
    prospectos_por_estado_chart = {'labels': [], 'data': []}
    remisiones_recientes = []
    hoy = datetime.now()

    # --- 1. Vencimientos KPIs ---
    ruta_vencimientos = app.config.get('VENCIMIENTOS_PROCESADA_FILE_PATH')
    if ruta_vencimientos and os.path.exists(ruta_vencimientos):
        try:
            df_venc = pd.read_excel(ruta_vencimientos)
            df_venc['FECHA FIN_dt'] = pd.to_datetime(df_venc['FECHA FIN'], errors='coerce')
            df_venc.dropna(subset=['FECHA FIN_dt'], inplace=True)
            df_venc['Dias_Para_Vencer'] = (df_venc['FECHA FIN_dt'] - hoy).dt.days

            # Aplicar el mismo filtro de ventana de 100 días que en el panel de vencimientos
            df_filtrado_venc = df_venc[df_venc['Dias_Para_Vencer'].between(-100, 100)]
            
            # Filtrar por pólizas que no están en estado final
            df_venc_activas = df_filtrado_venc[~df_filtrado_venc['Estado'].astype(str).str.lower().isin(['renovado', 'no renovado'])]

            # Excluir ramos especiales para alinear con el panel de vencimientos
            ramos_a_excluir = ['CUMPLIMIENTO', 'SERIEDAD DE OFERTA']
            df_venc_kpi = df_venc_activas[~df_venc_activas['RAMO PRINCIPAL'].isin(ramos_a_excluir)]

            kpis['vencimientos_15_dias'] = len(df_venc_kpi[df_venc_kpi['Dias_Para_Vencer'].between(0, 15)])
        except Exception as e:
            print(f"Error al calcular KPIs de vencimientos: {e}")

    # --- 2. Cobros Pendientes Mes KPI ---
    if os.path.exists(COBROS_FILE):
        try:
            df_cobros = pd.read_excel(COBROS_FILE)
            df_cobros['Fecha_Vencimiento_Cuota'] = pd.to_datetime(df_cobros['Fecha_Vencimiento_Cuota'], errors='coerce')
            df_cobros.dropna(subset=['Fecha_Vencimiento_Cuota'], inplace=True)

            # Replicar lógica de compatibilidad de panel_cobros para alinear los KPIs
            if 'Tipo_Movimiento' not in df_cobros.columns:
                df_cobros['Tipo_Movimiento'] = 'Cobro'
            df_cobros['Tipo_Movimiento'].fillna('Cobro', inplace=True)

            cobros_pendientes_mes = df_cobros[
                (df_cobros['Estado'] == 'Pendiente') &
                (df_cobros['Tipo_Movimiento'].str.contains('Cobro', case=False, na=False)) &
                (df_cobros['Fecha_Vencimiento_Cuota'].dt.month == hoy.month) &
                (df_cobros['Fecha_Vencimiento_Cuota'].dt.year == hoy.year)
            ]
            kpis['cobros_pendientes_mes'] = len(cobros_pendientes_mes)
        except Exception as e:
            print(f"Error al calcular KPIs de cobros: {e}")

    # --- 3. Prospectos KPIs & Chart ---
    prospectos_file_path = app.config.get('PROSPECTOS_FILE_PATH')
    if prospectos_file_path and os.path.exists(prospectos_file_path):
        try:
            df_prospectos = pd.read_excel(prospectos_file_path)
            kpis['prospectos_en_gestion'] = len(df_prospectos[df_prospectos['Estado'] == 'En gestión'])
            
            # Prospectos por estado chart data
            estado_counts = df_prospectos['Estado'].value_counts()
            prospectos_por_estado_chart['labels'] = estado_counts.index.tolist()
            prospectos_por_estado_chart['data'] = estado_counts.values.tolist()
        except Exception as e:
            print(f"Error al calcular KPIs de prospectos: {e}")

    # --- 4. Producción & Remisiones Recientes ---
    if os.path.exists(EXCEL_FILE):
        try:
            df_remisiones = pd.read_excel(EXCEL_FILE)
            df_remisiones['fecha_registro_dt'] = pd.to_datetime(df_remisiones['fecha_registro'], dayfirst=True, errors='coerce')
            
            # Producción del mes KPI y Chart
            produccion_mes_df = df_remisiones[
                (df_remisiones['estado'] == 'Creado') &
                (df_remisiones['fecha_registro_dt'].dt.month == hoy.month) &
                (df_remisiones['fecha_registro_dt'].dt.year == hoy.year)
            ]
            kpis['produccion_mes'] = produccion_mes_df['uib'].apply(limpiar_valor_moneda).sum()
            
            if not produccion_mes_df.empty:
                ramos_data = produccion_mes_df.groupby('ramo')['uib'].sum().sort_values(ascending=False).head(10)
                produccion_por_ramo_chart['labels'] = ramos_data.index.tolist()
                produccion_por_ramo_chart['data'] = ramos_data.values.tolist()

            # Remisiones recientes
            remisiones_recientes_df = df_remisiones.sort_values(by='fecha_registro_dt', ascending=False).head(5)
            remisiones_recientes = remisiones_recientes_df.to_dict(orient='records')

            # KPI de Remisiones Pendientes
            kpis['remisiones_pendientes'] = len(df_remisiones[df_remisiones['estado'] == 'Pendiente'])
        except Exception as e:
            print(f"Error al calcular KPIs de producción y remisiones: {e}")

    return render_template('index.html', 
                           kpis=kpis,
                           produccion_por_ramo_chart=produccion_por_ramo_chart,
                           prospectos_por_estado_chart=prospectos_por_estado_chart,
                           remisiones_recientes=remisiones_recientes)

@app.route('/carga_maestra', methods=['GET'])
@login_required
def mostrar_formulario_carga_maestra():
    return render_template('carga_maestra.html')

@app.route('/remision/nueva', methods=['GET'])
@login_required
def formulario_remision():
    # Obtener datos del prospecto desde la URL para autocompletar
    prospecto_data = {
        'tomador': request.args.get('tomador', ''),
        'ramo': request.args.get('ramo', ''),
        'aseguradora': request.args.get('aseguradora', ''),
        'prima_neta': request.args.get('prima_neta', ''),
        'poliza': request.args.get('poliza', '')
    }

    # These global lists should be defined at the top of app.py
    return render_template('formulario.html',
                           opciones_aseguradora=config_manager.get_list('aseguradoras'),
                           opciones_ramo=config_manager.get_list('ramos'),
                           opciones_tipo_moneda=config_manager.get_list('tipo_moneda'),
                           opciones_vendedor=config_manager.get_list('vendedores'),
                           opciones_forma_pago=config_manager.get_list('forma_pago'),
                           opciones_periodicidad_pago=config_manager.get_list('periodicidad_pago'),
                           opciones_analista=config_manager.get_list('analistas'),
                           prospecto=prospecto_data
                          )

@app.route('/registrar', methods=['POST'])
@login_required
def registrar():
    try:
        datos_formulario = request.form.to_dict()
        datos = {}

        # --- 1. Collect and Clean Data ---

        # Handle checkboxes
        checkbox_fields = ['renovacion', 'negocio_nuevo', 'renovable', 'modificacion', 'policy_number_modified']
        for field in checkbox_fields:
            datos[field] = "si" if field in datos_formulario else "no"
        # Handle anexo checkbox separately to avoid name conflict
        datos['anexo_checkbox'] = "si" if 'anexo_checkbox' in datos_formulario else "no"

        # Collect all other text/select form fields
        form_fields_to_collect = [
            'fecha_recepcion', 'tomador', 'nit', 'aseguradora', 'ramo', 'poliza', 'old_policy_number', 'anexo',
            'categorias_grupo', 'categorias_grupo_otro',
            'fecha_inicio', 'fecha_fin', 'fecha_limite_pago',
            'tipo_moneda',
            'vendedor', 'porcentaje_vendedor',
            'co_corretaje_opcion', 'co_corretaje_nombre', 'co_corretaje_porcentaje',
            'gastos_adicionales',
            'forma_pago', 'numero_cuotas', 'periodicidad_pago',
            'observaciones', 'riesgos_adicionales', 'analista_responsable'
        ]
        for field in form_fields_to_collect:
            datos[field] = datos_formulario.get(field, '').strip()

        # If co-corretaje is selected, the TPP seller is the one from the main 'vendedor' dropdown.
        if datos.get('co_corretaje_opcion') == 'si':
            datos['co_corretaje_nombre'] = datos.get('vendedor', '').strip()

        # Handle "Otro" for categorias_grupo
        if datos['categorias_grupo'] == 'Otro':
            datos['categorias_grupo'] = datos_formulario.get('categorias_grupo_otro', 'Otro').strip()

        # Clean numeric fields from form
        prima_neta = limpiar_valor_moneda(datos_formulario.get('prima_neta', '0'))
        porcentaje_comision = limpiar_valor_moneda(datos_formulario.get('porcentaje_comision_valor', '0'))
        porcentaje_vendedor = limpiar_valor_moneda(datos_formulario.get('porcentaje_vendedor', '0'))
        porcentaje_co_corretaje = limpiar_valor_moneda(datos_formulario.get('co_corretaje_porcentaje', '0')) # Keep for now, might be used elsewhere

        # --- 2. Backend Calculations (Corrected Logic) ---

        # Calculate Comision$
        comision_dolar = (prima_neta * porcentaje_comision) / 100.0

        # Calculate ComisionTPP based on the seller's participation
        comision_tpp = 0
        # If the seller is UIB, their commission is not TPP.
        if datos.get('vendedor') != 'UIB CORREDORES DE SEGUROS S.A.' and porcentaje_vendedor > 0 and datos.get('co_corretaje_opcion') == 'si':
             comision_tpp = comision_dolar * (porcentaje_vendedor / 100.0)

        # Calculate ComisionUIB, which is the remainder
        comision_uib = comision_dolar - comision_tpp

        # --- 3. Populate 'datos' dictionary for saving ---

        # Add cleaned and calculated values to the main dictionary
        datos['prima_neta'] = prima_neta
        datos['porcentaje_comision_valor'] = porcentaje_comision
        datos['porcentaje_vendedor'] = porcentaje_vendedor
        datos['co_corretaje_porcentaje'] = porcentaje_co_corretaje
        
        datos['Comision$'] = comision_dolar
        datos['ComisionTPP'] = comision_tpp
        datos['ComisionUIB'] = comision_uib
        datos['uib'] = comision_uib # This is the final UIB value

        # Add automatic and placeholder fields
        datos['consecutivo'] = obtener_consecutivo()
        datos['estado'] = 'Pendiente'
        datos['fecha_registro'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        datos['numero_remision_manual'] = '' # Initialize placeholder

        # --- 4. File Processing Logic ---
        nombre_cliente_form = datos.get('tomador', 'SIN_TOMADOR').strip()
        nit_cliente_form = datos.get('nit', 'SIN_NIT').strip()
        nombre_carpeta_cliente_base = f"{nombre_cliente_form}_{nit_cliente_form}"
        nombre_carpeta_cliente_seguro = secure_filename(nombre_carpeta_cliente_base)
        if not nombre_carpeta_cliente_seguro:
            nombre_carpeta_cliente_seguro = f"cliente_{datos.get('consecutivo', 'default_consec')}"

        ruta_base_cliente = os.path.join(app.config['CLIENT_FOLDERS_BASE_DIR'], nombre_carpeta_cliente_seguro)

        if not os.path.exists(ruta_base_cliente):
            os.makedirs(ruta_base_cliente, exist_ok=True)
            ano_actual_registro = datetime.now().strftime('%Y')
            subcarpetas_base_para_crear = [os.path.join("SARLAFT", ano_actual_registro), "POLIZAS", "DOCUMENTOS", "SINIESTROS"]
            for sub_base in subcarpetas_base_para_crear:
                os.makedirs(os.path.join(ruta_base_cliente, sub_base), exist_ok=True)

        archivos = request.files.getlist("archivos[]")
        tipos = request.form.getlist("tipo_archivo[]")
        otros_tipos_nombres = request.form.getlist("otro_tipo_nombre[]")
        nombres_archivos_guardados = []

        for i, archivo in enumerate(archivos):
            if archivo and archivo.filename:
                nombre_original = archivo.filename
                extension = os.path.splitext(nombre_original)[1].lower()
                tipo_original_seleccionado = tipos[i] if i < len(tipos) else "Otro"
                tipo_para_usar_en_nombre = ''
                if tipo_original_seleccionado == "Otro":
                    nombre_personalizado_otro = otros_tipos_nombres[i].strip() if i < len(otros_tipos_nombres) and otros_tipos_nombres[i].strip() else "OTRO_DOC"
                    tipo_para_usar_en_nombre = secure_filename(nombre_personalizado_otro.upper().replace(' ', '_'))
                    if not tipo_para_usar_en_nombre: tipo_para_usar_en_nombre = "OTRO_DOCUMENTO_SEGURO"
                else:
                    tipo_para_usar_en_nombre = secure_filename(tipo_original_seleccionado.replace(' ', '_'))

                ramo_form = datos.get('ramo', 'SIN_RAMO').replace(' ','_')

                # --- New, robust logic for vigencia_form to be YYYY-YYYY ---
                year_inicio = get_year_from_date(datos.get('fecha_inicio'))
                year_fin = get_year_from_date(datos.get('fecha_fin'))

                if year_inicio and year_fin:
                    vigencia_form = f"{year_inicio}-{year_fin}"
                else:
                    vigencia_form = 'SIN_VIGENCIA'

                ruta_destino_final = ''
                if tipo_original_seleccionado in ["Poliza", "Clausulado", "Recibo"]:
                    ruta_destino_final = os.path.join(ruta_base_cliente, 'POLIZAS', ramo_form, vigencia_form)
                else: # Includes 'Otro'
                    ruta_destino_final = os.path.join(ruta_base_cliente, 'DOCUMENTOS')

                os.makedirs(ruta_destino_final, exist_ok=True)

                nombre_base_archivo = f"{ramo_form}_{datos.get('poliza', 'SINPOLIZA')}_{datos.get('aseguradora', 'SINASEGURADORA')}_{tipo_para_usar_en_nombre}".replace(' ', '_')
                filename = secure_filename(nombre_base_archivo + extension)
                # Truncation logic can be added here if needed

                ruta_archivo_con_nombre = os.path.join(ruta_destino_final, filename)
                archivo.save(ruta_archivo_con_nombre)
                nombres_archivos_guardados.append(os.path.relpath(ruta_archivo_con_nombre, app.config['CLIENT_FOLDERS_BASE_DIR']))

        datos['archivos'] = ", ".join(nombres_archivos_guardados)

        if guardar_remision(datos):
            # --- Lógica para generar cuotas de cobro ---
            periodicidad = datos.get('periodicidad_pago')
            if datos.get('forma_pago') != 'Contado' and periodicidad in ['Mensual', 'Trimestral', 'Anual']:
                try:
                    num_cuotas = int(datos.get('numero_cuotas', 0))
                    if num_cuotas > 0:
                        nuevos_cobros = []
                        fecha_inicio_dt = datetime.strptime(datos.get('fecha_inicio'), '%Y-%m-%d')
                        
                        months_increment = 1 # Default para Mensual
                        if periodicidad == 'Trimestral':
                            months_increment = 3
                        elif periodicidad == 'Anual':
                            months_increment = 12

                        for i in range(num_cuotas):
                            fecha_vencimiento = fecha_inicio_dt + relativedelta(months=i * months_increment)

                            cobro = {
                                'ID_COBRO': uuid.uuid4().hex[:10].upper(),
                                'CONSECUTIVO_REMISION': datos.get('consecutivo'),
                                'Tomador': datos.get('tomador'),
                                'NIT_CC': datos.get('nit'),
                                'Aseguradora': datos.get('aseguradora'),
                                'Ramo': datos.get('ramo'),
                                'N_Poliza': datos.get('poliza'),
                                'N_Cuota': i + 1,
                                'Total_Cuotas': num_cuotas,
                                'Fecha_Vencimiento_Cuota': fecha_vencimiento.strftime('%Y-%m-%d'),
                                'Fecha_Inicio_Vigencia': datos.get('fecha_inicio'),
                                'Fecha_Fin_Vigencia': datos.get('fecha_fin'),
                                'Estado': 'Pendiente',
                                'Tipo_Movimiento': 'Cobro',
                                'Periodicidad': periodicidad
                            }
                            nuevos_cobros.append(cobro)

                        if nuevos_cobros:
                            guardar_cobros(nuevos_cobros)
                except (ValueError, TypeError) as e:
                    print(f"Error al procesar cuotas de cobro para {datos.get('consecutivo')}: {e}")

            return jsonify({'success': True, 'message': 'Remisión guardada exitosamente', 'consecutivo': datos.get('consecutivo')})
        else:
            return jsonify({'success': False, 'message': 'Error al guardar la remisión en Excel.'}), 500

    except Exception as e:
        print(f"Error en /registrar: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Error interno del servidor: {e}'}), 500

@app.route('/control')
@login_required
def control():
    remisiones_data = cargar_remisiones()
    remisiones_ordenadas = sorted(remisiones_data, key=lambda r: str(r.get('consecutivo', '')), reverse=True)
    primeras_10_remisiones = remisiones_ordenadas[:10]

    campos_esperados = [
        'consecutivo', 'fecha_recepcion', 'tomador', 'nit',
        'aseguradora', 'ramo', 'poliza',
        'fecha_inicio', 'fecha_fin', 'estado', 'numero_remision_manual',
        'archivos',
        'prima_neta',
        'Comision$',
        'ComisionTPP',
        'ComisionUIB',
        'uib'
    ]
    remisiones_list = []
    for r in primeras_10_remisiones:
        for campo in campos_esperados:
            if campo not in r:
                r[campo] = 'N/A'
        
        # FIX: Ensure 'archivos' is a string to prevent .split() error on float (NaN)
        if 'archivos' in r and not isinstance(r['archivos'], str):
            r['archivos'] = ''

        remisiones_list.append(r)
    return render_template('control.html', remisiones=remisiones_list)

@app.route('/marcar_creado', methods=['POST'])
@login_required
def marcar_creado():
    consecutivo_a_marcar = request.form.get('consecutivo')
    remisiones_actuales = cargar_remisiones()
    actualizado = False
    for remision in remisiones_actuales:
        if remision['consecutivo'] == consecutivo_a_marcar:
            remision['estado'] = 'Creado'
            actualizado = True
            break
    if actualizado:
        df = pd.DataFrame(remisiones_actuales)
        try:
            df.to_excel(EXCEL_FILE, index=False)
        except Exception as e:
            print(f"Error al guardar Excel después de marcar como creado: {e}")
    return redirect(url_for('control'))

@app.route('/plantilla', methods=['POST'])
@login_required
def plantilla():
    datos_formulario = request.form.to_dict()
    return render_template('plantilla_confirmacion.html', datos=datos_formulario)

@app.route('/resumen/<string:consecutivo_id>')
@login_required
def mostrar_resumen(consecutivo_id):
    remisiones = cargar_remisiones()
    remision_encontrada = None
    for r in remisiones:
        if str(r.get('consecutivo')) == str(consecutivo_id):
            remision_encontrada = r
            break
    if remision_encontrada:
        return render_template('resumen.html', datos=remision_encontrada)
    else:
        return f"Error: Remisión con consecutivo {consecutivo_id} no encontrada. Verifique el número o contacte soporte.", 404

@app.route('/editar_remision_numero/<string:consecutivo_id>', methods=['GET'])
@login_required
def editar_remision(consecutivo_id):
    remisiones = cargar_remisiones()
    remision_a_editar = None
    for r in remisiones:
        if str(r.get('consecutivo')).strip() == str(consecutivo_id).strip():
            remision_a_editar = r
            break

    if remision_a_editar:
        # Ensure all expected fields are present in the dictionary passed to the template
        # This prevents errors if the record in Excel is old and missing new columns.
        campos_completos = ORDEN_COLUMNAS_EXCEL_REMISIONES
        for campo in campos_completos:
            if campo not in remision_a_editar:
                remision_a_editar[campo] = '' # Default to empty string if missing

        return render_template('editar_remision.html', datos=remision_a_editar)
    else:
        return f"Error: Remisión con consecutivo {consecutivo_id} no encontrada. Verifique el número o contacte soporte.", 404

@app.route('/guardar_numero_remision', methods=['POST'])
@login_required
def guardar_numero_remision():
    consecutivo_a_actualizar = request.form.get('consecutivo')
    nuevo_numero_remision = request.form.get('numero_remision_manual', '').strip()
    if not consecutivo_a_actualizar:
        return "Error: Consecutivo no proporcionado para la actualización.", 400
    remisiones = cargar_remisiones()
    remisiones_actualizadas_df_list = []
    actualizacion_realizada = False
    for remision_data in remisiones:
        if str(remision_data.get('consecutivo')).strip() == str(consecutivo_a_actualizar).strip():
            remision_data['numero_remision_manual'] = nuevo_numero_remision
            actualizacion_realizada = True
        remisiones_actualizadas_df_list.append(remision_data)
    if actualizacion_realizada:
        df = pd.DataFrame(remisiones_actualizadas_df_list)
        try:
            if 'ORDEN_COLUMNAS_EXCEL_REMISIONES' in globals() and isinstance(ORDEN_COLUMNAS_EXCEL_REMISIONES, list):
                current_cols_rem = df.columns.tolist()
                for col_rem_maestra in ORDEN_COLUMNAS_EXCEL_REMISIONES:
                    if col_rem_maestra not in current_cols_rem:
                        # Initialize missing columns. Most remisiones fields are strings or can be empty strings.
                        # Specific numeric fields might need 0, but for general remisiones data, "" is safer if unsure.
                        df[col_rem_maestra] = ""
                # Enforce the exact order, dropping any columns not in the master list (though not expected here)
                df = df.reindex(columns=ORDEN_COLUMNAS_EXCEL_REMISIONES).fillna('')
            else:
                print("ADVERTENCIA en guardar_numero_remision: ORDEN_COLUMNAS_EXCEL_REMISIONES no definida. Remisiones se guardará con orden actual.")

            df.to_excel(EXCEL_FILE, index=False)
            # flash(f'Número de remisión para {consecutivo_a_actualizar} guardado.', 'success') # Example original flash
        except Exception as e:
            print(f"Error al guardar Excel en /guardar_numero_remision: {e}")
            return f"Error crítico al intentar guardar los cambios en el archivo Excel: {e}. Por favor, contacte soporte.", 500

        # --- Inicia lógica para actualizar vencimientos asociados ---
        # 'actualizacion_realizada' should be True if the above save was successful.
        # 'nuevo_numero_remision' is from request.form
        # 'numero_poliza_de_remision' should have been extracted from df for the updated row.
        # 'consecutivo_a_actualizar' is also available.

        if actualizacion_realizada and nuevo_numero_remision.strip() : # Only proceed if a non-empty numero_remision_manual was set
            numero_poliza_a_buscar = None
            remision_actualizada_data = df[df['consecutivo'] == consecutivo_a_actualizar].iloc[0]

            # Check if the policy number was modified and use the old one if available
            policy_modified_flag = remision_actualizada_data.get('policy_number_modified')
            old_policy_number = remision_actualizada_data.get('old_policy_number')

            if policy_modified_flag == 'si' and pd.notna(old_policy_number) and str(old_policy_number).strip():
                # Convert to int to remove decimals, then to string for matching.
                try:
                    numero_poliza_a_buscar = str(int(float(old_policy_number))).strip()
                except (ValueError, TypeError):
                    # Fallback if conversion fails for some reason
                    numero_poliza_a_buscar = str(old_policy_number).strip()
            else:
                numero_poliza_a_buscar = str(remision_actualizada_data.get('poliza', '')).strip()

            if numero_poliza_a_buscar and numero_poliza_a_buscar not in ['N/A', 'None', '', 'nan', 'NaN']:
                ruta_vencimientos = app.config.get('VENCIMIENTOS_PROCESADA_FILE_PATH')
                if ruta_vencimientos and os.path.exists(ruta_vencimientos):
                    try:
                        df_vencimientos = pd.read_excel(ruta_vencimientos)
                        vencimientos_modificados_count = 0

                        if 'NÚMERO PÓLIZA' in df_vencimientos.columns:
                            # Ensure consistent string comparison
                            df_vencimientos['NÚMERO PÓLIZA_str_comp'] = df_vencimientos['NÚMERO PÓLIZA'].astype(str).str.strip().fillna('')

                            filas_afectadas_mask = df_vencimientos['NÚMERO PÓLIZA_str_comp'] == numero_poliza_a_buscar

                            if filas_afectadas_mask.any():
                                if 'Estado' not in df_vencimientos.columns: df_vencimientos['Estado'] = ''
                                if 'Remision_Asociada' not in df_vencimientos.columns: df_vencimientos['Remision_Asociada'] = ''
                                if 'Observaciones_adicionales' not in df_vencimientos.columns: df_vencimientos['Observaciones_adicionales'] = ''

                                df_vencimientos.loc[filas_afectadas_mask, 'Estado'] = "Renovado"
                                df_vencimientos.loc[filas_afectadas_mask, 'Remision_Asociada'] = nuevo_numero_remision
                                df_vencimientos.loc[filas_afectadas_mask, 'Observaciones_adicionales'] = f"Remisión: {nuevo_numero_remision}"
                                vencimientos_modificados_count = filas_afectadas_mask.sum()

                            if 'NÚMERO PÓLIZA_str_comp' in df_vencimientos.columns: # Drop helper column
                                df_vencimientos = df_vencimientos.drop(columns=['NÚMERO PÓLIZA_str_comp'])
                        else:
                            print(f"Advertencia: Columna 'NÚMERO PÓLIZA' no encontrada en {ruta_vencimientos} al intentar actualizar vencimientos.")

                        if vencimientos_modificados_count > 0:
                            if 'ORDEN_COLUMNAS_VENCIMIENTOS' in globals() and isinstance(ORDEN_COLUMNAS_VENCIMIENTOS, list):
                                for col_m_v in ORDEN_COLUMNAS_VENCIMIENTOS:
                                    if col_m_v not in df_vencimientos.columns:
                                        default_val_venc = 0 if col_m_v == 'ID_VENCIMIENTO' else ''
                                        df_vencimientos[col_m_v] = default_val_venc
                                df_vencimientos = df_vencimientos[ORDEN_COLUMNAS_VENCIMIENTOS]
                            else:
                                print("ADVERTENCIA: ORDEN_COLUMNAS_VENCIMIENTOS no definida. Vencimientos se guardará con orden actual.")

                            df_vencimientos.to_excel(ruta_vencimientos, index=False)
                            flash(f'{vencimientos_modificados_count} registro(s) de vencimiento para póliza "{numero_poliza_a_buscar}" actualizados a "Renovado" (Remisión: {nuevo_numero_remision}).', 'info')

                    except Exception as e_venc:
                        print(f"Error al actualizar vencimientos asociados: {type(e_venc).__name__} - {e_venc}")
                        flash(f'N° Remisión guardado, pero ocurrió un error al intentar actualizar vencimientos asociados: {str(e_venc)}', 'warning')
                elif ruta_vencimientos and not os.path.exists(ruta_vencimientos):
                    flash('Archivo de vencimientos no encontrado. No se pudieron actualizar estados de vencimiento.', 'info')
                elif not (numero_poliza_a_buscar and numero_poliza_a_buscar not in ['N/A', 'None', '', 'nan', 'NaN']):
                     flash('No se proporcionó un número de póliza válido en la remisión, no se actualizaron vencimientos.', 'info')

        elif actualizacion_realizada and not nuevo_numero_remision.strip():
            flash('Número de remisión manual está vacío, no se intentó actualizar vencimientos.', 'info')
        # --- Fin lógica para actualizar vencimientos ---
    else:
        return f"Error: No se encontró la remisión con consecutivo {consecutivo_a_actualizar} para actualizar. Es posible que haya sido eliminada.", 404
    return redirect(url_for('control'))

@app.route('/formulario_crear_carpeta', methods=['GET'])
@login_required
def mostrar_formulario_crear_carpeta():
    ano_actual = datetime.now().strftime('%Y')
    return render_template('crear_carpeta.html', ano_actual=ano_actual)

@app.route('/ejecutar_crear_carpeta', methods=['POST'])
@login_required
def ejecutar_crear_carpeta():
    tipo_carpeta = request.form.get('tipo_carpeta')
    nombre_entidad = request.form.get('nombre_entidad', 'SIN_NOMBRE').strip()
    nit_entidad = request.form.get('nit_entidad', 'SIN_NIT').strip()
    ano_actual = datetime.now().strftime('%Y')

    if not nombre_entidad or nombre_entidad == 'SIN_NOMBRE' or not nit_entidad or nit_entidad == 'SIN_NIT':
        return jsonify({'success': False, 'message': 'El nombre y el NIT/CC son obligatorios.'}), 400

    nombre_carpeta_base = f"{nombre_entidad}_{nit_entidad}"
    nombre_carpeta_seguro = secure_filename(nombre_carpeta_base)

    if not nombre_carpeta_seguro:
        return jsonify({'success': False, 'message': 'El nombre o NIT/CC generan un nombre de carpeta inválido.'}), 400

    if tipo_carpeta == 'cliente':
        ruta_base = app.config['CLIENT_FOLDERS_BASE_DIR']
        ruta_completa = os.path.join(ruta_base, nombre_carpeta_seguro)
        subcarpetas = [os.path.join("SARLAFT", ano_actual), "POLIZAS", "DOCUMENTOS", "SINIESTROS"]
        try:
            if not os.path.exists(ruta_completa):
                os.makedirs(ruta_completa)
            for subcarpeta_rel in subcarpetas:
                os.makedirs(os.path.join(ruta_completa, subcarpeta_rel), exist_ok=True)

            ruta_sarlaft_ano = os.path.join(ruta_completa, "SARLAFT", ano_actual)
            documentos_sarlaft_config = {
                'doc_cedula': 'Cedula_Representante_legal', 'doc_sarlaft': 'Sarlaft_Cliente',
                'doc_rut': 'RUT_Cliente', 'doc_declaracion': 'Declaracion_Renta', 'doc_camara': 'Camara_Comercio',
                'estados_financieros': 'Estados_Financieros_Notas', 'consulta_cliente': 'Consulta_Cliente_Desqubra',
            }
            archivos_cargados_count = 0
            for input_name, nombre_base_fijo in documentos_sarlaft_config.items():
                archivo = request.files.get(input_name)
                if archivo and archivo.filename:
                    extension = os.path.splitext(archivo.filename)[1].lower()
                    nombre_archivo_final = nombre_base_fijo + extension
                    ruta_guardado = os.path.join(ruta_sarlaft_ano, secure_filename(nombre_archivo_final))
                    archivo.save(ruta_guardado)
                    archivos_cargados_count += 1
            mensaje_exito = f'Estructura de carpetas para cliente "{nombre_entidad}" creada/verificada.'
            if archivos_cargados_count > 0:
                mensaje_exito += f' {archivos_cargados_count} documento(s) SARLAFT procesados.'
            return jsonify({'success': True, 'message': mensaje_exito}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error al crear carpetas de cliente: {e}'}), 500

    elif tipo_carpeta == 'vendedor':
        ruta_base = app.config['VENDEDOR_FOLDERS_BASE_DIR']
        ruta_completa = os.path.join(ruta_base, nombre_carpeta_seguro)
        try:
            os.makedirs(ruta_completa, exist_ok=True)
            archivos = request.files.getlist("documentos_vendedor[]")
            archivos_cargados_count = 0
            for archivo in archivos:
                if archivo and archivo.filename:
                    filename = secure_filename(archivo.filename)
                    archivo.save(os.path.join(ruta_completa, filename))
                    archivos_cargados_count += 1
            mensaje_exito = f'Carpeta para vendedor "{nombre_entidad}" creada. {archivos_cargados_count} documento(s) subidos.'
            return jsonify({'success': True, 'message': mensaje_exito}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error al crear carpeta de vendedor: {e}'}), 500
    else:
        return jsonify({'success': False, 'message': 'Tipo de carpeta no válido.'}), 400

@app.route('/prospectos/crear', methods=['GET', 'POST'])
@login_required
def crear_prospecto():
    if request.method == 'GET':
        return render_template('prospectos_crear.html',
                               opciones_responsable_tecnico=config_manager.get_list('responsable_tecnico'),
                               opciones_responsable_comercial=config_manager.get_list('responsable_comercial'),
                               opciones_estado=config_manager.get_list('estado_prospecto'),
                               opciones_ramo=config_manager.get_list('ramos'),
                               opciones_aseguradora=config_manager.get_list('aseguradoras'),
                               opciones_vendedor=config_manager.get_list('vendedores'))

    if request.method == 'POST':
        try:
            datos_formulario = request.form.to_dict()

            # Calcular Comision $
            prima_str = datos_formulario.get('Prima', '0')
            prima = limpiar_valor_moneda(prima_str)
            datos_formulario['Prima'] = prima # Store the cleaned numeric value

            comision_porcentaje = float(datos_formulario.get('Comision %', 0))
            comision_calculada = prima * (comision_porcentaje / 100)

            if datos_formulario.get('es_TPP') == 'si':
                porcentaje_tpp = float(datos_formulario.get('Porcentaje_comision_TPP', 0))
                comision_calculada -= comision_calculada * (porcentaje_tpp / 100)

            datos_formulario['Comision $'] = comision_calculada

            PROSPECTOS_FILE = app.config['PROSPECTOS_FILE_PATH']

            if os.path.exists(PROSPECTOS_FILE):
                df_prospectos = pd.read_excel(PROSPECTOS_FILE)
            else:
                df_prospectos = pd.DataFrame(columns=ORDEN_COLUMNAS_PROSPECTOS)

            datos_formulario['ID_PROSPECTO'] = uuid.uuid4().hex[:8].upper()
            datos_formulario['Fecha Creacion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            new_prospect_df = pd.DataFrame([datos_formulario])

            df_prospectos = pd.concat([df_prospectos, new_prospect_df], ignore_index=True)

            df_prospectos = df_prospectos[ORDEN_COLUMNAS_PROSPECTOS]

            df_prospectos.to_excel(PROSPECTOS_FILE, index=False)

            return jsonify({'status': 'success', 'message': 'Prospecto guardado exitosamente'})

        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})

@app.route('/prospectos/visualizar', methods=['GET'])
@login_required
def prospectos_vista():
    try:
        PROSPECTOS_FILE = app.config['PROSPECTOS_FILE_PATH']
        kpi_recaudo_mes = 0
        kpi_top_ramos = []

        if os.path.exists(PROSPECTOS_FILE):
            df = pd.read_excel(PROSPECTOS_FILE)

            # --- Data Cleaning and Preparation ---
            df['Fecha inicio poliza'] = pd.to_datetime(df['Fecha inicio poliza'], errors='coerce')
            if 'Fecha Creacion' in df.columns:
                df['Fecha Creacion'] = pd.to_datetime(df['Fecha Creacion'], errors='coerce')

            currency_cols = ['Prima', 'Comision $']
            for col in currency_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

            # --- KPI Calculation (Ganado en el mes actual) ---
            hoy = datetime.now()
            df_ganado_mes_actual = df[
                (df['Estado'] == 'Ganado') &
                (df['Fecha inicio poliza'].dt.month == hoy.month) &
                (df['Fecha inicio poliza'].dt.year == hoy.year)
            ]

            kpi_recaudo_mes = df_ganado_mes_actual['Comision $'].sum()

            top_ramos = df_ganado_mes_actual.groupby('Ramo')['Comision $'].sum().nlargest(3).reset_index()
            kpi_top_ramos = top_ramos.to_dict(orient='records')

            # --- Sorting (más reciente primero) ---
            if 'Fecha Creacion' in df.columns:
                df.sort_values(by='Fecha Creacion', ascending=False, inplace=True)
            else:
                # Fallback to ID if Fecha Creacion doesn't exist yet
                df.sort_values(by='ID_PROSPECTO', ascending=False, inplace=True)

            prospectos_data = df.head(20).to_dict(orient='records')
        else:
            prospectos_data = []

    except Exception as e:
        flash(f'Error al cargar los prospectos: {str(e)}', 'danger')
        prospectos_data = []

    return render_template('prospectos_vista.html',
                           prospectos=prospectos_data,
                           opciones_responsable_tecnico=config_manager.get_list('responsable_tecnico'),
                           opciones_responsable_comercial=config_manager.get_list('responsable_comercial'),
                           opciones_estado=config_manager.get_list('estado_prospecto'),
                           kpi_recaudo_mes=kpi_recaudo_mes,
                           kpi_top_ramos=kpi_top_ramos)

@app.route('/prospectos/editar/<prospecto_id>')
@login_required
def prospecto_editar(prospecto_id):
    PROSPECTOS_FILE = app.config['PROSPECTOS_FILE_PATH']
    if not os.path.exists(PROSPECTOS_FILE):
        flash('El archivo de prospectos no existe.', 'danger')
        return redirect(url_for('prospectos_vista'))

    df = pd.read_excel(PROSPECTOS_FILE, dtype={'ID_PROSPECTO': str})
    prospecto_data = df[df['ID_PROSPECTO'] == prospecto_id].to_dict('records')

    if not prospecto_data:
        flash('Prospecto no encontrado.', 'danger')
        return redirect(url_for('prospectos_vista'))

    return render_template('prospectos_editar.html',
                           prospecto=prospecto_data[0],
                           opciones_responsable_tecnico=config_manager.get_list('responsable_tecnico'),
                           opciones_responsable_comercial=config_manager.get_list('responsable_comercial'),
                           opciones_estado=config_manager.get_list('estado_prospecto'),
                           opciones_ramo=config_manager.get_list('ramos'),
                           opciones_aseguradora=config_manager.get_list('aseguradoras'),
                           opciones_vendedor=config_manager.get_list('vendedores'))

@app.route('/prospectos/guardar_edicion', methods=['POST'])
@login_required
def prospecto_guardar_edicion():
    try:
        datos = request.form.to_dict()
        prospecto_id = datos.get('ID_PROSPECTO')

        PROSPECTOS_FILE = app.config['PROSPECTOS_FILE_PATH']
        df = pd.read_excel(PROSPECTOS_FILE, dtype={'ID_PROSPECTO': str})

        index_list = df[df['ID_PROSPECTO'] == prospecto_id].index
        if not index_list.any():
            flash('Prospecto no encontrado para actualizar.', 'danger')
            return redirect(url_for('prospectos_vista'))

        idx = index_list[0]

        for key, value in datos.items():
            if key in df.columns:
                df.loc[idx, key] = value

        # Recalculate commission
        prima_str = str(df.loc[idx, 'Prima'])
        prima = limpiar_valor_moneda(prima_str)
        df.loc[idx, 'Prima'] = prima # Save cleaned value back

        comision_porc = float(df.loc[idx, 'Comision %'])
        comision_calculada = prima * (comision_porc / 100)

        if str(df.loc[idx, 'es_TPP']) == 'si':
            porcentaje_tpp = float(df.loc[idx, 'Porcentaje_comision_TPP'])
            comision_calculada -= comision_calculada * (porcentaje_tpp / 100)

        df.loc[idx, 'Comision $'] = comision_calculada

        df.to_excel(PROSPECTOS_FILE, index=False)
        flash('Prospecto actualizado con éxito.', 'success')

    except Exception as e:
        flash(f'Error al guardar el prospecto: {e}', 'danger')

    return redirect(url_for('prospectos_vista'))

@app.route('/prospectos/actualizar_estado', methods=['POST'])
@login_required
def actualizar_estado_prospecto():
    try:
        data = request.get_json()
        prospecto_id = data.get('prospecto_id')
        nuevo_estado = data.get('estado')

        if not prospecto_id or nuevo_estado not in ['Ganado', 'Perdido']:
            return jsonify({'status': 'error', 'message': 'Datos inválidos.'}), 400

        PROSPECTOS_FILE = app.config['PROSPECTOS_FILE_PATH']

        if not os.path.exists(PROSPECTOS_FILE):
            return jsonify({'status': 'error', 'message': 'El archivo de prospectos no existe.'}), 404

        df = pd.read_excel(PROSPECTOS_FILE, dtype={'ID_PROSPECTO': str})

        index = df[df['ID_PROSPECTO'] == str(prospecto_id)].index

        if not index.empty:
            df.loc[index, 'Estado'] = nuevo_estado

            fecha_emision = None
            if nuevo_estado == 'Ganado':
                fecha_emision = datetime.now().strftime('%Y-%m-%d')
                if 'Fecha inicio poliza' not in df.columns:
                    df['Fecha inicio poliza'] = ''
                df.loc[index, 'Fecha inicio poliza'] = fecha_emision

            df.to_excel(PROSPECTOS_FILE, index=False)

            response = {'status': 'success', 'message': f'Prospecto marcado como {nuevo_estado}.'}
            if fecha_emision:
                response['fecha_emision'] = fecha_emision
            return jsonify(response)
        else:
            return jsonify({'status': 'error', 'message': 'Prospecto no encontrado.'}), 404

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

import locale

# Set locale to Spanish for month names
try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'es')
    except locale.Error:
        print("Locale 'es_ES' or 'es' not found. Month names will be in English.")

def format_date_in_spanish(date_str):
    if not date_str or pd.isna(date_str):
        return ""
    try:
        # First, try to parse with slashes, then with hyphens
        try:
            dt = datetime.strptime(str(date_str), '%d/%m/%Y')
        except ValueError:
            dt = datetime.strptime(str(date_str), '%Y-%m-%d')
        return dt.strftime('%d de %B de %Y')
    except (ValueError, TypeError):
        return date_str # Return original if format is unexpected

@app.route('/correspondencia/vista_previa')
@login_required
def correspondencia_vista_previa():
    try:
        datos = request.args.to_dict()
        consecutivo = datos.get('consecutivo')
        tipo_plantilla = datos.get('tipo_plantilla')

        if not consecutivo or not tipo_plantilla:
            return "Error: Faltan parámetros.", 400

        remisiones_df = pd.read_excel(EXCEL_FILE, dtype={'consecutivo': str})
        remision_data = remisiones_df[remisiones_df['consecutivo'] == consecutivo].to_dict('records')

        if not remision_data:
            return "Error: Remisión no encontrada.", 404

        contexto = remision_data[0]
        contexto.update(datos)

        # Generar asunto automático
        ref_asunto_auto = f"{contexto.get('consecutivo', '')} | {contexto.get('tomador', '')} | {contexto.get('ramo', '')} No {contexto.get('poliza', '')}"

        # Limpiar nombres de archivos para la descripción
        archivos_str = contexto.get('archivos', '')
        if archivos_str:
            # Extraer solo el nombre base del archivo sin la ruta y la extensión
            nombres_limpios = [os.path.splitext(os.path.basename(f))[0].split('_')[-1] for f in archivos_str.split(',')]
            descripcion_archivos = ' - '.join(nombres_limpios)
        else:
            descripcion_archivos = ''

        contexto_plantilla = {
            'empresa': contexto.get('tomador'),
            'sr_sra': contexto.get('sr_sra'),
            'correo': contexto.get('correo'),
            'fecha': datetime.now().strftime('%d de %B de %Y'),
            'consecutivo': contexto.get('consecutivo'),
            'ref_asunto': ref_asunto_auto,
            'aseguradora': contexto.get('aseguradora'),
            'fecha_inicio': format_date_in_spanish(contexto.get('fecha_inicio')),
            'fecha_terminacion': format_date_in_spanish(contexto.get('fecha_fin')),
            'ramo': contexto.get('ramo'),
            'poliza': contexto.get('poliza'),
            'descripcion': descripcion_archivos,
            'valor_a_pagar': contexto.get('valor_a_pagar'),
            'garantias': contexto.get('garantias'),
            'fecha_de_pago': format_date_in_spanish(contexto.get('fecha_limite_pago')),
            'link_de_pago': contexto.get('link_de_pago')
        }

        return render_template(f'correspondencia/{tipo_plantilla}.html', **contexto_plantilla)

    except Exception as e:
        return f"Error al generar la vista previa: {e}", 500

@app.route('/siniestros/registrar', methods=['GET', 'POST'])
@login_required
def siniestros_registrar():
    if request.method == 'GET':
        return render_template('siniestros_registrar.html')

    if request.method == 'POST':
        try:
            datos = request.form.to_dict()
            archivos = request.files.getlist('documentos')

            # --- 1. Guardar registro en siniestros.xlsx ---
            SINIESTROS_FILE = os.path.join(BASE_DIR, 'siniestros.xlsx')
            nombres_archivos = [secure_filename(f.filename) for f in archivos if f.filename]
            datos['archivos_adjuntos'] = ', '.join(nombres_archivos)

            df_siniestros_existente = pd.DataFrame()
            if os.path.exists(SINIESTROS_FILE):
                df_siniestros_existente = pd.read_excel(SINIESTROS_FILE)

            nuevo_siniestro_df = pd.DataFrame([datos])
            df_final = pd.concat([df_siniestros_existente, nuevo_siniestro_df], ignore_index=True)
            df_final.to_excel(SINIESTROS_FILE, index=False)

            # --- 2. Subir archivos a carpetas ---
            nombre_cliente = secure_filename(datos['nombre_cliente'])
            nit_cc = secure_filename(datos['nit_cc'])
            ramo = secure_filename(datos['ramo'])
            ano_siniestro = datetime.strptime(datos['fecha_siniestro'], '%Y-%m-%d').strftime('%Y')

            ruta_carpeta_cliente = os.path.join(app.config['CLIENT_FOLDERS_BASE_DIR'], f"{nombre_cliente}_{nit_cc}")
            ruta_destino = os.path.join(ruta_carpeta_cliente, 'SINIESTROS', ramo, ano_siniestro)

            os.makedirs(ruta_destino, exist_ok=True)

            for archivo in archivos:
                if archivo and archivo.filename:
                    filename = secure_filename(archivo.filename)
                    archivo.save(os.path.join(ruta_destino, filename))

            return jsonify({'status': 'success', 'message': 'Siniestro registrado y archivos subidos exitosamente.'})

        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'status': 'error', 'message': f'Error interno del servidor: {e}'}), 500

@app.route('/cartera/visualizar', methods=['GET'])
@login_required
def visualizar_cartera():
    ruta_archivo_procesado = app.config['CARTERA_PROCESADA_FILE_PATH']

    if not os.path.exists(ruta_archivo_procesado):
        flash('No hay reporte de cartera procesado. Por favor, cargue uno primero.', 'warning')
        return redirect(url_for('mostrar_formulario_carga_maestra'))

    try:
        df = pd.read_excel(ruta_archivo_procesado)

        anos_disponibles = []
        if 'FECHA CREACIÓN' in df.columns:
            df['FECHA CREACIÓN_dt'] = pd.to_datetime(df['FECHA CREACIÓN'], format='%d/%m/%Y', errors='coerce')
            if pd.api.types.is_datetime64_any_dtype(df['FECHA CREACIÓN_dt']):
                anos_disponibles = sorted(df['FECHA CREACIÓN_dt'].dt.year.dropna().unique().astype(int), reverse=True)
            else:
                flash('La columna "FECHA CREACIÓN" no pudo ser interpretada como fecha para extraer años.', 'warning')
        else:
            flash('Columna "FECHA CREACIÓN" no encontrada, no se puede filtrar por año/mes.', 'danger')

        aseguradoras_disponibles = []
        if 'ASEGURADORA' in df.columns:
            aseguradoras_disponibles = sorted(df['ASEGURADORA'].dropna().unique().astype(str))
        else:
            flash('Columna "ASEGURADORA" no encontrada, no se puede filtrar por aseguradora.', 'warning')

        ano_seleccionado_str = request.args.get('ano_filtro')
        mes_seleccionado_str = request.args.get('mes_filtro')
        aseguradora_seleccionada_actual = request.args.get('aseguradora_filtro')
        ano_seleccionado_int = None
        mes_seleccionado_int = None

        if ano_seleccionado_str and ano_seleccionado_str.isdigit():
            ano_seleccionado_int = int(ano_seleccionado_str)
            if 'FECHA CREACIÓN_dt' in df.columns and pd.api.types.is_datetime64_any_dtype(df['FECHA CREACIÓN_dt']):
                df = df[df['FECHA CREACIÓN_dt'].dt.year == ano_seleccionado_int]
            elif 'FECHA CREACIÓN_dt' not in df.columns or not pd.api.types.is_datetime64_any_dtype(df['FECHA CREACIÓN_dt']):
                 flash('No se pudo filtrar por año debido a problemas con la columna "FECHA CREACIÓN".', 'warning')

        if mes_seleccionado_str and mes_seleccionado_str.isdigit():
            mes_seleccionado_int = int(mes_seleccionado_str)
            if 1 <= mes_seleccionado_int <= 12:
                if 'FECHA CREACIÓN_dt' in df.columns and pd.api.types.is_datetime64_any_dtype(df['FECHA CREACIÓN_dt']):
                    df = df[df['FECHA CREACIÓN_dt'].dt.month == mes_seleccionado_int]
                elif 'FECHA CREACIÓN_dt' not in df.columns or not pd.api.types.is_datetime64_any_dtype(df['FECHA CREACIÓN_dt']):
                    flash(f'No se pudo filtrar por mes. Problemas con "FECHA CREACIÓN".', 'warning')
            else:
                mes_seleccionado_int = None

        if aseguradora_seleccionada_actual and 'ASEGURADORA' in df.columns:
            df = df[df['ASEGURADORA'] == aseguradora_seleccionada_actual]

        df_display = df.copy()
        columnas_moneda = [
            'PRIMA NETA', 'COMISIÓN',
            'Retencion_Calc', 'Reteica_Calc',
            'Valor_Comision_UIB_Neto_Calc',
            'Valor_Comision_Intermediario_Calc'
        ]
        columnas_porcentaje = [
            'PORCENTAJE DE COMISIÓN',
            'Porc_Com_Intermediario_Original'
        ]

        for col in columnas_moneda:
            if col in df_display.columns:
                df_display[col] = pd.to_numeric(df_display[col], errors='coerce').fillna(0.0)
                if col in ['Reteica_Calc', 'Retencion_Calc']:
                    df_display[col] = df_display[col].apply(lambda x: f"$ {x:,.2f}" if pd.notnull(x) else "$ 0.00")
                else:
                    df_display[col] = df_display[col].apply(lambda x: f"$ {x:,.0f}" if pd.notnull(x) else "$ 0")

        for col in columnas_porcentaje:
            if col in df_display.columns:
                df_display[col] = pd.to_numeric(df_display[col], errors='coerce').fillna(0.0)
                df_display[col] = df_display[col].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "0.0%")

        if 'FECHA CREACIÓN_dt' in df_display.columns and pd.api.types.is_datetime64_any_dtype(df_display['FECHA CREACIÓN_dt']):
            df_display['FECHA CREACIÓN'] = df_display['FECHA CREACIÓN_dt'].dt.strftime('%Y-%m-%d').replace('NaT', '')
        elif 'FECHA CREACIÓN' in df_display.columns:
              df_display['FECHA CREACIÓN'] = df_display['FECHA CREACIÓN'].astype(str).fillna('')

        if 'FECHA CREACIÓN_dt' in df_display.columns:
            df_display = df_display.drop(columns=['FECHA CREACIÓN_dt'])

        df_display = df_display.fillna('')
        lista_remisiones = df_display.to_dict(orient='records')

        nombres_meses_template = [
            (1, "Enero"), (2, "Febrero"), (3, "Marzo"), (4, "Abril"),
            (5, "Mayo"), (6, "Junio"), (7, "Julio"), (8, "Agosto"),
            (9, "Septiembre"), (10, "Octubre"), (11, "Noviembre"), (12, "Diciembre")
        ]

        return render_template('cartera_vista.html',
                               remisiones=lista_remisiones,
                               meses_para_filtro=nombres_meses_template,
                               anos_disponibles_filtro=anos_disponibles,
                               aseguradoras_disponibles_filtro=aseguradoras_disponibles,
                               mes_seleccionado_actual_int=mes_seleccionado_int,
                               ano_seleccionado_actual_int=ano_seleccionado_int,
                               aseguradora_seleccionada_actual=aseguradora_seleccionada_actual)

    except Exception as e:
        print(f"Error al visualizar el reporte de cartera: {e}")
        flash(f'Error al leer o mostrar el archivo de cartera: {str(e)}', 'danger')
        return redirect(url_for('mostrar_formulario_carga_maestra'))

@app.route('/cartera/editar/<int:id_registro>', methods=['GET'])
@login_required
def mostrar_formulario_editar_cartera(id_registro):
    ruta_archivo_procesado = app.config['CARTERA_PROCESADA_FILE_PATH']
    if not os.path.exists(ruta_archivo_procesado):
        flash('Archivo de cartera procesada no encontrado. Por favor, cargue un reporte primero.', 'danger')
        return redirect(url_for('visualizar_cartera'))

    try:
        df = pd.read_excel(ruta_archivo_procesado)
        # ID_CARTERA fue guardado como int, id_registro viene como int de la URL
        registro_para_editar_df = df[df['ID_CARTERA'] == id_registro]

        if registro_para_editar_df.empty:
            flash(f'No se encontró el registro de cartera con ID {id_registro}.', 'warning')
            return redirect(url_for('visualizar_cartera'))

        registro_dict = registro_para_editar_df.iloc[0].fillna('').to_dict()

        return render_template('cartera_editar_registro.html', registro=registro_dict)

    except Exception as e:
        print(f"Error al cargar registro de cartera para editar: {e}")
        flash(f'Error al cargar el registro para edición: {str(e)}', 'danger')
        return redirect(url_for('visualizar_cartera'))

@app.route('/cartera/guardar_edicion', methods=['POST'])
@login_required
def guardar_edicion_cartera():
    id_cartera_actualizar = request.form.get('id_cartera')
    if not id_cartera_actualizar:
        flash('ID de cartera no proporcionado. No se pudo guardar.', 'danger')
        return redirect(url_for('visualizar_cartera'))

    try:
        id_cartera_actualizar = int(id_cartera_actualizar) # Convertir ID a int
    except ValueError:
        flash('ID de cartera inválido.', 'danger')
        return redirect(url_for('visualizar_cartera'))

    # Nuevos valores desde el formulario
    n_factura_manual = request.form.get('N_FACTURA_Manual', '').strip()
    clasificacion_manual = request.form.get('Clasificacion_Manual', '').strip()
    line_of_business_manual = request.form.get('Line_of_Business_Manual', '').strip()

    ruta_archivo_procesado = app.config['CARTERA_PROCESADA_FILE_PATH']
    if not os.path.exists(ruta_archivo_procesado):
        flash('Archivo de cartera procesada no encontrado. No se pudo guardar.', 'danger')
        return redirect(url_for('visualizar_cartera'))

    try:
        df = pd.read_excel(ruta_archivo_procesado)

        columnas_manuales_a_asegurar_str = ['N_FACTURA_Manual', 'Clasificacion_Manual', 'Line_of_Business_Manual']
        for col in columnas_manuales_a_asegurar_str:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('')
            else:
                df[col] = pd.Series([''] * len(df), index=df.index, dtype=object)

        # Encontrar el índice de la fila a actualizar
        indice_fila = df[df['ID_CARTERA'] == id_cartera_actualizar].index

        if not indice_fila.empty:
            idx = indice_fila[0]
            # Actualizar los campos
            df.loc[idx, 'N_FACTURA_Manual'] = n_factura_manual
            df.loc[idx, 'Clasificacion_Manual'] = clasificacion_manual
            df.loc[idx, 'Line_of_Business_Manual'] = line_of_business_manual

            # Ensure all columns from the master order list exist and enforce order
            for col_maestra in ORDEN_COLUMNAS_EXCEL_CARTERA:
                if col_maestra not in df.columns:
                    if '_Calc' in col_maestra or col_maestra in ['PRIMA NETA', 'COMISIÓN', 'PORCENTAJE DE COMISIÓN', 'ID_CARTERA']: # ID_CARTERA is numeric
                        df[col_maestra] = 0
                    else:
                        df[col_maestra] = pd.Series([''] * len(df), index=df.index, dtype=object)
            df = df[ORDEN_COLUMNAS_EXCEL_CARTERA]

            # Guardar el DataFrame modificado
            df.to_excel(ruta_archivo_procesado, index=False)
            flash(f'Registro de cartera ID {id_cartera_actualizar} actualizado exitosamente.', 'success')
        else:
            flash(f'No se encontró el registro de cartera con ID {id_cartera_actualizar} para actualizar.', 'warning')

    except Exception as e:
        print(f"Error al guardar edición de cartera: {e}")
        flash(f'Error al guardar los cambios: {str(e)}', 'danger')

    return redirect(url_for('visualizar_cartera'))

@app.route('/cartera/aplicar_factura_lote', methods=['POST'])
@login_required
def aplicar_factura_lote():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'Error: No se recibieron datos en la solicitud.'}), 400

        ids_registros_str = data.get('ids_registros')
        numero_factura = data.get('numero_factura', '').strip()

        if not ids_registros_str or not isinstance(ids_registros_str, list) or len(ids_registros_str) == 0:
            return jsonify({'success': False, 'message': 'Error: No se seleccionaron registros para la actualización.'}), 400

        if not numero_factura:
            return jsonify({'success': False, 'message': 'Error: El número de factura no puede estar vacío.'}), 400

        try:
            ids_registros_int = [int(id_str) for id_str in ids_registros_str]
        except ValueError:
            return jsonify({'success': False, 'message': 'Error: IDs de registro contienen valores no válidos.'}), 400

        ruta_archivo_procesado = app.config['CARTERA_PROCESADA_FILE_PATH']
        if not os.path.exists(ruta_archivo_procesado):
            return jsonify({'success': False, 'message': 'Error crítico: Archivo de cartera procesada no encontrado en el servidor.'}), 500

        df = pd.read_excel(ruta_archivo_procesado)

        if 'ID_CARTERA' not in df.columns:
            return jsonify({'success': False, 'message': 'Error de configuración: La columna ID_CARTERA no se encontró en el archivo Excel.'}), 500

        # Asegurar que la columna ID_CARTERA en el DataFrame sea del mismo tipo que los IDs recibidos (int)
        try:
            df['ID_CARTERA'] = pd.to_numeric(df['ID_CARTERA'], errors='raise').astype(int)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'message': 'Error de datos: La columna ID_CARTERA en el Excel contiene valores no numéricos o no puede ser convertida a entero.'}), 500

        # Encontrar los índices de las filas a actualizar
        filas_a_actualizar_mask = df['ID_CARTERA'].isin(ids_registros_int)
        indices_filas_a_actualizar = df[filas_a_actualizar_mask].index

        if len(indices_filas_a_actualizar) == 0:
            return jsonify({'success': False, 'message': 'Advertencia: Ninguno de los IDs de registro seleccionados fue encontrado en el archivo de cartera. No se realizaron cambios.'}), 404 # Not Found or Bad Request

        # Actualizar la columna N_FACTURA_Manual para las filas encontradas
        if 'N_FACTURA_Manual' in df.columns:
            df['N_FACTURA_Manual'] = df['N_FACTURA_Manual'].astype(str).fillna('')
        else:
            df['N_FACTURA_Manual'] = pd.Series([''] * len(df), index=df.index, dtype=object) # Create if missing

        df.loc[indices_filas_a_actualizar, 'N_FACTURA_Manual'] = numero_factura

        # Ensure all columns from the master order list exist and enforce order
        for col_maestra in ORDEN_COLUMNAS_EXCEL_CARTERA:
            if col_maestra not in df.columns:
                if '_Calc' in col_maestra or col_maestra in ['PRIMA NETA', 'COMISIÓN', 'PORCENTAJE DE COMISIÓN', 'ID_CARTERA']:
                    df[col_maestra] = 0
                else:
                    df[col_maestra] = pd.Series([''] * len(df), index=df.index, dtype=object)
        df = df[ORDEN_COLUMNAS_EXCEL_CARTERA]

        df.to_excel(ruta_archivo_procesado, index=False)

        return jsonify({'success': True, 'message': f'{len(indices_filas_a_actualizar)} registro(s) fueron actualizados exitosamente con el N° de Factura: {numero_factura}.'}), 200

    except Exception as e:
        print(f"Error crítico en aplicar_factura_lote: {type(e).__name__} - {e}")
        # Para el usuario, un mensaje más genérico puede ser mejor
        return jsonify({'success': False, 'message': f'Ocurrió un error interno en el servidor al intentar aplicar la factura al lote. Por favor, contacte soporte.'}), 500

@app.route('/cartera/descargar_reporte_final', methods=['GET'])
@login_required
def descargar_reporte_cartera_final():
    try:
        ruta_archivo = app.config['CARTERA_PROCESADA_FILE_PATH']

        if not os.path.exists(ruta_archivo):
            flash('No se encontró el archivo de cartera procesada para descargar. Por favor, procese un reporte primero.', 'danger')
            return redirect(url_for('visualizar_cartera'))

        download_filename = 'Reporte_Cartera_Final_UIB.xlsx'

        return send_file(
            ruta_archivo,
            as_attachment=True,
            download_name=download_filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        print(f"Error al intentar descargar el reporte final de cartera: {type(e).__name__} - {e}")
        flash(f'Ocurrió un error al generar la descarga del reporte: {str(e)}', 'danger')
        return redirect(url_for('visualizar_cartera'))

@app.route('/vencimientos/visualizar', methods=['GET'])
@login_required
def visualizar_vencimientos():
    ruta_archivo_vencimientos = app.config['VENCIMIENTOS_PROCESADA_FILE_PATH']

    if not os.path.exists(ruta_archivo_vencimientos):
        flash('No hay reporte de vencimientos procesado. Por favor, cargue uno primero.', 'warning')
        return redirect(url_for('mostrar_formulario_carga_maestra'))

    try:
        df_venc = pd.read_excel(ruta_archivo_vencimientos)
        df_venc.rename(columns={'NOMBRES CLIENTE': 'Tomador'}, inplace=True)

        if 'FECHA FIN' not in df_venc.columns:
            flash('El archivo de vencimientos no contiene la columna "FECHA FIN".', 'danger')
            return render_template('vencimientos_vista.html', registros=[], kpis={}, ramos_kpis=[], search_term='')

        df_venc['FECHA FIN_dt'] = pd.to_datetime(df_venc['FECHA FIN'], errors='coerce')
        df_venc.dropna(subset=['FECHA FIN_dt'], inplace=True)

        hoy = datetime.now()
        df_venc['Dias_Para_Vencer'] = (df_venc['FECHA FIN_dt'] - hoy).dt.days

        # Filtrar por defecto a pólizas vencidas en los últimos 100 días y por vencer en los próximos 100 días.
        df_filtrado = df_venc[(df_venc['Dias_Para_Vencer'] >= -100) & (df_venc['Dias_Para_Vencer'] <= 100)].copy()

        # Get search term
        search_term = request.args.get('search_term', '').strip()
        if search_term:
            df_filtrado = df_filtrado[df_filtrado['Tomador'].str.contains(search_term, case=False, na=False)]

        # Excluir 'CUMPLIMIENTO' y 'SERIEDAD DE OFERTA' de los KPIs generales
        ramos_a_excluir_kpi_general = ['CUMPLIMIENTO', 'SERIEDAD DE OFERTA']
        df_para_kpis_generales = df_filtrado[~df_filtrado['RAMO PRINCIPAL'].isin(ramos_a_excluir_kpi_general)]

        # Calcular KPIs
        kpis = {
            'vencer_15_dias': len(df_para_kpis_generales[(df_para_kpis_generales['Dias_Para_Vencer'] >= 0) & (df_para_kpis_generales['Dias_Para_Vencer'] <= 15)]),
            'vencer_30_dias': len(df_para_kpis_generales[(df_para_kpis_generales['Dias_Para_Vencer'] >= 0) & (df_para_kpis_generales['Dias_Para_Vencer'] <= 30)]),
            'vencer_60_dias': len(df_para_kpis_generales[(df_para_kpis_generales['Dias_Para_Vencer'] >= 0) & (df_para_kpis_generales['Dias_Para_Vencer'] <= 60)]),
            'vencidas': len(df_para_kpis_generales[df_para_kpis_generales['Dias_Para_Vencer'] < 0]),
            'cumplimiento': len(df_filtrado[(df_filtrado['RAMO PRINCIPAL'] == 'CUMPLIMIENTO') & (df_filtrado['Dias_Para_Vencer'] >= 0) & (df_filtrado['Dias_Para_Vencer'] <= 45)])
        }

        # Calcular KPIs por ramo (usando el dataframe ya filtrado)
        df_vencer_30_dias = df_filtrado[(df_filtrado['Dias_Para_Vencer'] >= 0) & (df_filtrado['Dias_Para_Vencer'] <= 30)]
        ramos_kpis = [
            {
                'ramo': 'AUTOS/VEHÍCULOS',
                'count': len(df_vencer_30_dias[df_vencer_30_dias['RAMO PRINCIPAL'].str.contains('AUTOS|VEHICULOS', case=False, na=False)])
            },
            {
                'ramo': 'COPROPIEDADES',
                'count': len(df_vencer_30_dias[df_vencer_30_dias['RAMO PRINCIPAL'].str.contains('COPROPIEDADES', case=False, na=False)])
            },
            {
                'ramo': 'HOGAR',
                'count': len(df_vencer_30_dias[df_vencer_30_dias['RAMO PRINCIPAL'].str.contains('HOGAR', case=False, na=False)])
            },
            {
                'ramo': 'ARRENDAMIENTO',
                'count': len(df_vencer_30_dias[df_vencer_30_dias['RAMO PRINCIPAL'].str.contains('ARRENDAMIENTO', case=False, na=False)])
            }
        ]

        def determinar_alerta(dias, estado_actual):
            estado_actual_lower = str(estado_actual).lower()

            # Final states with specific icons
            if estado_actual_lower == 'renovado':
                return {'icon': 'fas fa-check-double', 'css_class': 'alerta-renovado', 'text': 'Renovado', 'is_critical': False}
            elif estado_actual_lower == 'no renovado':
                return {'icon': 'fas fa-ban', 'css_class': 'alerta-no-renovado', 'text': 'No Renovado', 'is_critical': False}

            if pd.isna(dias):
                return {'icon': 'fas fa-question-circle', 'css_class': 'alerta-gris', 'text': 'Fecha Fin Inválida', 'is_critical': False}

            dias = int(dias)
            
            # Defaults
            css_class = 'alerta-azul'
            icon = 'fas fa-info-circle'
            is_critical = False

            # Critical case
            if dias <= 5:
                is_critical = True

            # Semaphore logic for color AND icon
            if dias <= 10:
                css_class = 'alerta-rojo'
                icon = 'fas fa-skull-crossbones'
            elif dias <= 20: # 11-20 days
                css_class = 'alerta-amarillo'
                icon = 'fas fa-exclamation-triangle'
            elif dias <= 30: # 21-30 days
                css_class = 'alerta-verde'
                icon = 'fas fa-calendar-check'

            if dias < 0:
                text = f'Vencido (Hace {-dias} días)'
            else:
                text = f'Vence en {dias} días'
            
            return {'icon': icon, 'css_class': css_class, 'text': text, 'is_critical': is_critical}

        if 'Estado' not in df_filtrado.columns:
            df_filtrado['Estado'] = ''
        else:
            df_filtrado['Estado'] = df_filtrado['Estado'].astype(str).fillna('')

        # Se aplica la función para determinar la alerta sobre el DataFrame ya filtrado
        resultados_alerta = df_filtrado.apply(lambda row: determinar_alerta(row.get('Dias_Para_Vencer'), row.get('Estado')), axis=1)

        # Se asignan los resultados al DataFrame filtrado.
        df_filtrado['Indicador_Vencimiento_CSS_Class'] = [a['css_class'] for a in resultados_alerta]
        df_filtrado['Indicador_Vencimiento_Icon'] = [a['icon'] for a in resultados_alerta]
        df_filtrado['Indicador_Vencimiento_Text'] = [a['text'] for a in resultados_alerta]
        df_filtrado['Indicador_Vencimiento_Is_Critical'] = [a['is_critical'] for a in resultados_alerta]

        df_display = df_filtrado.copy()

        # Ordenar por 'Dias_Para_Vencer' de mayor a menor
        df_display.sort_values(by='Dias_Para_Vencer', ascending=False, inplace=True)

        # Formatear fechas para mostrar
        df_display['FECHA FIN'] = df_display['FECHA FIN_dt'].dt.strftime('%Y-%m-%d')
        if 'Fecha_inicio_seguimiento' in df_display.columns:
             df_display['Fecha_inicio_seguimiento'] = pd.to_datetime(df_display['Fecha_inicio_seguimiento'], errors='coerce').dt.strftime('%Y-%m-%d')

        df_display = df_display.fillna('')
        lista_registros = df_display.to_dict(orient='records')

        return render_template('vencimientos_vista.html',
                                registros=lista_registros,
                                kpis=kpis,
                                ramos_kpis=ramos_kpis,
                                opciones_responsable_js=config_manager.get_list('responsable_vencimientos'),
                                opciones_estado_js=config_manager.get_list('estado_vencimientos'),
                                search_term=search_term
                               )
    except Exception as e:
        print(f"Error crítico al visualizar el reporte de vencimientos: {type(e).__name__} - {e}")
        flash(f'Ocurrió un error crítico al intentar mostrar el reporte de vencimientos: {str(e)}', 'danger')
        return redirect(url_for('mostrar_formulario_carga_maestra'))

@app.route('/vencimientos/actualizar_registro', methods=['POST'])
@login_required
def actualizar_registro_vencimiento():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No se recibieron datos JSON.'}), 400

        id_vencimiento = data.get('id_vencimiento')
        # Obtener los nuevos valores, usando .get() con default por si no vienen todos los campos
        nuevo_responsable = data.get('Responsable', '').strip()
        nuevo_estado = data.get('Estado', '').strip()
        nuevas_observaciones = data.get('Observaciones_adicionales', '').strip()

        if id_vencimiento is None: # Acepta ID 0 pero no None
            return jsonify({'success': False, 'message': 'ID de vencimiento no proporcionado.'}), 400

        try:
            id_vencimiento = int(id_vencimiento) # El ID viene del data-id que es un value de un input
        except ValueError:
            return jsonify({'success': False, 'message': 'ID de vencimiento inválido (no es un número).'}), 400

        ruta_archivo_vencimientos = app.config['VENCIMIENTOS_PROCESADA_FILE_PATH']
        if not os.path.exists(ruta_archivo_vencimientos):
            return jsonify({'success': False, 'message': 'Archivo de datos de vencimientos no encontrado en el servidor.'}), 500

        df = pd.read_excel(ruta_archivo_vencimientos)

        if 'ID_VENCIMIENTO' not in df.columns:
            return jsonify({'success': False, 'message': 'Error crítico: Columna ID_VENCIMIENTO no encontrada en el archivo Excel.'}), 500

        try:
            df['ID_VENCIMIENTO'] = df['ID_VENCIMIENTO'].astype(int)
        except ValueError:
            return jsonify({'success': False, 'message': 'Error de datos: La columna ID_VENCIMIENTO en Excel contiene valores no numéricos.'}), 500

        # Encontrar el índice de la fila a actualizar
        indice_fila_arr = df[df['ID_VENCIMIENTO'] == id_vencimiento].index

        if not indice_fila_arr.empty:
            idx = indice_fila_arr[0]
            # Actualizar los campos. Asegurar que las columnas sean de tipo string si no existen.
            for col_name, new_value in [('Responsable', nuevo_responsable),
                                        ('Estado', nuevo_estado),
                                        ('Observaciones_adicionales', nuevas_observaciones)]:
                if col_name not in df.columns:
                    df[col_name] = pd.Series(dtype='object') # Crear como object para strings
                df.loc[idx, col_name] = new_value

            # Reordenar columnas antes de guardar para mantener consistencia
            # (ORDEN_COLUMNAS_VENCIMIENTOS debe estar definida globalmente)
            # global ORDEN_COLUMNAS_VENCIMIENTOS # No es necesario declarar global si solo se lee
            if 'ORDEN_COLUMNAS_VENCIMIENTOS' in globals() and isinstance(ORDEN_COLUMNAS_VENCIMIENTOS, list):
                # Asegurar que todas las columnas del orden existen en el df
                for col_maestra in ORDEN_COLUMNAS_VENCIMIENTOS:
                    if col_maestra not in df.columns:
                        # Initialize missing columns; ID_VENCIMIENTO should exist from read or previous processing
                        df[col_maestra] = pd.Series([''] * len(df), dtype=object) if col_maestra not in ['ID_VENCIMIENTO'] else pd.Series(dtype='int64', index=df.index)


                df = df[ORDEN_COLUMNAS_VENCIMIENTOS]
            else:
                print("ADVERTENCIA: ORDEN_COLUMNAS_VENCIMIENTOS no está definida o no es una lista. El Excel se guardará con el orden actual del DataFrame.")

            df.to_excel(ruta_archivo_vencimientos, index=False)
            print(f"INFO: Archivo de vencimientos guardado en {ruta_archivo_vencimientos} después de actualizar ID {id_vencimiento}.")
            return jsonify({'success': True, 'message': f'Registro de vencimiento ID {id_vencimiento} actualizado exitosamente.'}), 200
        else:
            print(f"WARN: No se encontró el ID_VENCIMIENTO {id_vencimiento} para actualizar.")
            return jsonify({'success': False, 'message': f'Error: No se encontró el registro de vencimiento con ID {id_vencimiento}.'}), 404

    except FileNotFoundError:
        return jsonify({'success': False, 'message': 'Error crítico: Archivo de datos no encontrado durante la actualización.'}), 500
    except pd.errors.EmptyDataError:
        return jsonify({'success': False, 'message': 'Error: El archivo de datos de vencimientos está vacío o corrupto.'}), 500
    except Exception as e:
        print(f"Error en actualizar_registro_vencimiento: {type(e).__name__} - {e}")
        return jsonify({'success': False, 'message': f'Ocurrió un error interno en el servidor: {str(e)}'}), 500

@app.route('/procesar_reporte_maestro', methods=['POST'])
@login_required
def procesar_reporte_maestro():
    # --- 1. File Upload Validation ---
    if 'archivo' not in request.files:
        flash('No se encontró el archivo en la solicitud.', 'danger')
        return redirect(url_for('mostrar_formulario_carga_maestra'))
    archivo = request.files['archivo']
    if archivo.filename == '':
        flash('No se seleccionó ningún archivo.', 'warning')
        return redirect(url_for('mostrar_formulario_carga_maestra'))
    if not (archivo.filename.endswith('.xlsx') or archivo.filename.endswith('.xls')):
        flash('Formato de archivo no válido. Suba un Excel (.xlsx o .xls).', 'warning')
        return redirect(url_for('mostrar_formulario_carga_maestra'))

    try:
        df_maestro = pd.read_excel(archivo)
    except Exception as e:
        flash(f'Error al leer el archivo maestro Excel: {str(e)}', 'danger')
        return redirect(url_for('mostrar_formulario_carga_maestra'))

    # --- 2. Cartera Module Logic ---
    try:
        ruta_cartera = app.config['CARTERA_PROCESADA_FILE_PATH']
        columnas_faltantes_cartera = [col for col in COLUMNAS_A_EXTRAER_CARTERA if col not in df_maestro.columns]
        if columnas_faltantes_cartera:
            cols_str = ", ".join(columnas_faltantes_cartera)
            flash(f'Columnas de Cartera faltantes en archivo maestro: {cols_str}. No se procesó Cartera.', 'warning')
        else:
            # --- Corrected Unique Key Creation ---
            df_maestro['CLAVE_UNICA'] = df_maestro['NÚMERO PÓLIZA'].astype(str).str.strip() + "_" + pd.to_datetime(df_maestro['FECHA CREACIÓN'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d').fillna('NODATE')

            df_cartera_existente = pd.DataFrame()
            if os.path.exists(ruta_cartera):
                df_cartera_existente = pd.read_excel(ruta_cartera)
                if not df_cartera_existente.empty and 'NÚMERO PÓLIZA' in df_cartera_existente.columns and 'FECHA CREACIÓN' in df_cartera_existente.columns:
                    df_cartera_existente['CLAVE_UNICA'] = df_cartera_existente['NÚMERO PÓLIZA'].astype(str).str.strip() + "_" + df_cartera_existente['FECHA CREACIÓN'].astype(str).str.strip()

            # --- Data Processing ---
            df_cartera_procesados_nuevos = df_maestro[COLUMNAS_A_EXTRAER_CARTERA + ['CLAVE_UNICA']].copy()
            # (Calculation logic remains the same)
            temp_porc_com = df_cartera_procesados_nuevos['PORCENTAJE DE COMISIÓN'].astype(str).str.replace('%', '', regex=False).str.replace(',', '.', regex=False).str.strip()
            df_cartera_procesados_nuevos['PORCENTAJE DE COMISIÓN_num'] = pd.to_numeric(temp_porc_com, errors='coerce').fillna(0.0)
            df_cartera_procesados_nuevos['COMISIÓN_num'] = pd.to_numeric(df_cartera_procesados_nuevos['COMISIÓN'], errors='coerce').fillna(0.0)
            df_cartera_procesados_nuevos['Retencion_Calc'] = df_cartera_procesados_nuevos['COMISIÓN_num'] * 0.11
            df_cartera_procesados_nuevos['Reteica_Calc'] = df_cartera_procesados_nuevos['COMISIÓN_num'] * 0.0014
            df_cartera_procesados_nuevos['Valor_Comision_UIB_Neto_Calc'] = df_cartera_procesados_nuevos['COMISIÓN_num'] - df_cartera_procesados_nuevos['Retencion_Calc'] - df_cartera_procesados_nuevos['Reteica_Calc']
            df_cartera_procesados_nuevos['Intermediario_Original'] = df_cartera_procesados_nuevos['VENDEDOR'].astype(str).fillna('')
            df_cartera_procesados_nuevos['Porc_Com_Intermediario_Original'] = df_cartera_procesados_nuevos['PORCENTAJE DE COMISIÓN_num']
            df_cartera_procesados_nuevos['Valor_Comision_Intermediario_Calc'] = df_cartera_procesados_nuevos['Valor_Comision_UIB_Neto_Calc'] * (df_cartera_procesados_nuevos['Porc_Com_Intermediario_Original'] / 100.0)
            df_cartera_procesados_nuevos['COMISIÓN'] = df_cartera_procesados_nuevos['COMISIÓN_num']
            df_cartera_procesados_nuevos['PORCENTAJE DE COMISIÓN'] = df_cartera_procesados_nuevos['PORCENTAJE DE COMISIÓN_num']
            df_cartera_procesados_nuevos.drop(columns=[col for col in df_cartera_procesados_nuevos.columns if col.endswith('_num')], inplace=True, errors='ignore')

            # --- Merge Logic ---
            if not df_cartera_existente.empty:
                nuevos_registros_mask = ~df_cartera_procesados_nuevos['CLAVE_UNICA'].isin(df_cartera_existente['CLAVE_UNICA'])
                df_nuevos_para_anadir = df_cartera_procesados_nuevos[nuevos_registros_mask]
                df_para_actualizar = df_cartera_procesados_nuevos[~nuevos_registros_mask]
            else:
                df_nuevos_para_anadir = df_cartera_procesados_nuevos
                df_para_actualizar = pd.DataFrame()

            if not df_para_actualizar.empty and not df_cartera_existente.empty:
                columnas_desde_maestro = COLUMNAS_A_EXTRAER_CARTERA + COLUMNAS_CALCULADAS_CARTERA
                cols_para_update = [col for col in columnas_desde_maestro if col in df_cartera_existente.columns and col in df_para_actualizar.columns]
                df_cartera_existente.set_index('CLAVE_UNICA', inplace=True)
                df_para_actualizar.set_index('CLAVE_UNICA', inplace=True)
                df_cartera_existente.update(df_para_actualizar[cols_para_update])
                df_cartera_existente.reset_index(inplace=True)

            df_cartera_final = pd.concat([df_cartera_existente, df_nuevos_para_anadir], ignore_index=True, sort=False)

            # --- Finalize and Save ---
            if 'ID_CARTERA' in df_cartera_final.columns:
                df_cartera_final.drop(columns=['ID_CARTERA'], inplace=True, errors='ignore')
            df_cartera_final.insert(0, 'ID_CARTERA', range(1, len(df_cartera_final) + 1))

            for col in ORDEN_COLUMNAS_EXCEL_CARTERA:
                if col not in df_cartera_final.columns:
                    df_cartera_final[col] = ''
            df_cartera_final = df_cartera_final[ORDEN_COLUMNAS_EXCEL_CARTERA]

            df_cartera_final.to_excel(ruta_cartera, index=False)
            flash(f'Módulo Cartera actualizado: {len(df_nuevos_para_anadir)} registros nuevos añadidos, {len(df_para_actualizar)} registros existentes actualizados.', 'success')
    except Exception as e_cartera:
        flash(f'Error procesando la sección de Cartera del archivo maestro: {str(e_cartera)}', 'danger')

    # --- 3. Logic for Vencimientos Module ---
    try:
        if 'ESTADO' in df_maestro.columns:
            df_maestro = df_maestro[df_maestro['ESTADO'] == 'Vigente'].copy()
        else:
            flash('La columna "ESTADO" no se encontró en el archivo maestro, no se pudo filtrar por pólizas vigentes.', 'warning')

        ruta_vencimientos = app.config['VENCIMIENTOS_PROCESADA_FILE_PATH']
        columnas_faltantes_venc = [col for col in COLUMNAS_A_EXTRAER_VENCIMIENTOS if col not in df_maestro.columns]
        if columnas_faltantes_venc:
            cols_str_venc = ", ".join(columnas_faltantes_venc)
            flash(f'Columnas de Vencimientos faltantes en archivo maestro: {cols_str_venc}. No se procesó Vencimientos.', 'warning')
        else:
            # --- Unique Key Creation for Vencimientos ---
            df_maestro.drop_duplicates(subset=['NÚMERO PÓLIZA', 'FECHA FIN'], keep='first', inplace=True)
            df_maestro['CLAVE_UNICA_VENC'] = df_maestro['NÚMERO PÓLIZA'].astype(str).str.strip() + "_" + pd.to_datetime(df_maestro['FECHA FIN'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d').fillna('NODATE_VENC')

            df_venc_existente = pd.DataFrame()
            if os.path.exists(ruta_vencimientos):
                df_venc_existente = pd.read_excel(ruta_vencimientos)
                if not df_venc_existente.empty and 'NÚMERO PÓLIZA' in df_venc_existente.columns and 'FECHA FIN' in df_venc_existente.columns:
                    df_venc_existente['CLAVE_UNICA_VENC'] = df_venc_existente['NÚMERO PÓLIZA'].astype(str).str.strip() + "_" + pd.to_datetime(df_venc_existente['FECHA FIN'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('NODATE_VENC_EXIST')

            # --- Data Processing for Vencimientos ---
            df_venc_procesados_nuevos = df_maestro[COLUMNAS_A_EXTRAER_VENCIMIENTOS + ['CLAVE_UNICA_VENC']].copy()
            df_venc_procesados_nuevos['FECHA FIN_dt'] = pd.to_datetime(df_venc_procesados_nuevos['FECHA FIN'], format='%d/%m/%Y', errors='coerce')
            df_venc_procesados_nuevos['Fecha_inicio_seguimiento'] = (df_venc_procesados_nuevos['FECHA FIN_dt'] - pd.Timedelta(days=30)).dt.strftime('%Y-%m-%d')
            df_venc_procesados_nuevos['FECHA FIN'] = df_venc_procesados_nuevos['FECHA FIN_dt'].dt.strftime('%Y-%m-%d')
            df_venc_procesados_nuevos.drop(columns=['FECHA FIN_dt'], inplace=True, errors='ignore')

            # --- Merge Logic for Vencimientos ---
            if not df_venc_existente.empty:
                nuevos_registros_mask_venc = ~df_venc_procesados_nuevos['CLAVE_UNICA_VENC'].isin(df_venc_existente['CLAVE_UNICA_VENC'])
                df_nuevos_para_anadir_venc = df_venc_procesados_nuevos[nuevos_registros_mask_venc]
                df_para_actualizar_venc = df_venc_procesados_nuevos[~nuevos_registros_mask_venc]
            else:
                df_nuevos_para_anadir_venc = df_venc_procesados_nuevos
                df_para_actualizar_venc = pd.DataFrame()

            if not df_para_actualizar_venc.empty and not df_venc_existente.empty:
                columnas_desde_maestro_venc = COLUMNAS_A_EXTRAER_VENCIMIENTOS + ['Fecha_inicio_seguimiento']
                cols_para_update_venc = [col for col in columnas_desde_maestro_venc if col in df_venc_existente.columns and col in df_para_actualizar_venc.columns]
                df_venc_existente.set_index('CLAVE_UNICA_VENC', inplace=True)
                df_para_actualizar_venc.set_index('CLAVE_UNICA_VENC', inplace=True)
                df_venc_existente.update(df_para_actualizar_venc[cols_para_update_venc])
                df_venc_existente.reset_index(inplace=True)

            df_venc_final = pd.concat([df_venc_existente, df_nuevos_para_anadir_venc], ignore_index=True, sort=False)

            # --- Finalize and Save Vencimientos ---
            if 'ID_VENCIMIENTO' in df_venc_final.columns:
                df_venc_final.drop(columns=['ID_VENCIMIENTO'], inplace=True, errors='ignore')
            df_venc_final.insert(0, 'ID_VENCIMIENTO', range(1, len(df_venc_final) + 1))

            for col in ORDEN_COLUMNAS_VENCIMIENTOS:
                if col not in df_venc_final.columns:
                    df_venc_final[col] = ''
            df_venc_final = df_venc_final[ORDEN_COLUMNAS_VENCIMIENTOS]

            df_venc_final.to_excel(ruta_vencimientos, index=False)
            flash(f'Módulo Vencimientos actualizado: {len(df_nuevos_para_anadir_venc)} registros nuevos añadidos, {len(df_para_actualizar_venc)} registros existentes actualizados.', 'success')

    except Exception as e_venc:
        flash(f'Error procesando la sección de Vencimientos del archivo maestro: {str(e_venc)}', 'danger')

    return redirect(url_for('index')) # Final redirect

@app.route('/recaudo')
@login_required
def recaudo():
    # Initialize all variables
    total_renovaciones, total_prospectos, total_modificaciones, total_general = 0, 0, 0, 0
    renovaciones_data, prospectos_data, modificaciones_data = [], [], []
    tpp_por_vendedor = []
    chart_data = {'labels': [], 'data': []}

    remisiones = cargar_remisiones()
    if remisiones:
        df = pd.DataFrame(remisiones)

        # Define all columns that will be used
        required_cols = [
            'renovacion', 'negocio_nuevo', 'modificacion', 'estado', 'fecha_registro', 
            'uib', 'ramo', 'ComisionUIB', 'co_corretaje_opcion', 'ComisionTPP', 'co_corretaje_nombre'
        ]
        
        # Check if all required columns exist
        missing_cols = [col for col in required_cols if col not in df.columns]
        if not missing_cols:
            # Clean and prepare data
            df['uib'] = df['uib'].apply(limpiar_valor_moneda)
            df['ComisionUIB'] = df['ComisionUIB'].apply(limpiar_valor_moneda)
            df['ComisionTPP'] = df['ComisionTPP'].apply(limpiar_valor_moneda)
            df['fecha_registro_dt'] = pd.to_datetime(df['fecha_registro'], dayfirst=True, errors='coerce')
            df.dropna(subset=['fecha_registro_dt'], inplace=True)

            hoy = datetime.now()

            # Base filter for remisiones created in the current month
            base_filter = (
                (df['estado'].astype(str).str.strip().str.lower() == 'creado') &
                (df['fecha_registro_dt'].dt.month == hoy.month) &
                (df['fecha_registro_dt'].dt.year == hoy.year)
            )
            df_mes_actual = df[base_filter].copy()

            # --- Calculations for KPI cards (using ComisionUIB as per user feedback) ---
            df_renovaciones = df_mes_actual[df_mes_actual['renovacion'].astype(str).str.strip().str.lower() == 'si']
            total_renovaciones = df_renovaciones['ComisionUIB'].sum()
            renovaciones_data = df_renovaciones.to_dict(orient='records')

            df_prospectos = df_mes_actual[df_mes_actual['negocio_nuevo'].astype(str).str.strip().str.lower() == 'si']
            total_prospectos = df_prospectos['ComisionUIB'].sum()
            prospectos_data = df_prospectos.to_dict(orient='records')

            df_modificaciones = df_mes_actual[df_mes_actual['modificacion'].astype(str).str.strip().str.lower() == 'si']
            total_modificaciones = df_modificaciones['ComisionUIB'].sum()
            modificaciones_data = df_modificaciones.to_dict(orient='records')

            total_general = total_renovaciones + total_prospectos + total_modificaciones

            # --- TPP per Seller Calculation (Corrected Logic) ---
            df_tpp = df_mes_actual[df_mes_actual['co_corretaje_opcion'].astype(str).str.strip().str.lower() == 'si'].copy()
            if not df_tpp.empty:
                # Fill NaN values, convert to string, and strip whitespace to handle empty/null seller names correctly.
                df_tpp['co_corretaje_nombre'] = df_tpp['co_corretaje_nombre'].fillna('').astype(str).str.strip()
                
                # Replace any resulting empty strings with a descriptive placeholder.
                df_tpp.loc[df_tpp['co_corretaje_nombre'] == '', 'co_corretaje_nombre'] = 'Vendedor no especificado'

                comisiones_tpp_agrupadas = df_tpp.groupby('co_corretaje_nombre')['ComisionTPP'].sum().reset_index()
                
                # Filter out 'UIB' and sellers with zero commission
                comisiones_tpp_filtradas = comisiones_tpp_agrupadas[
                    (~comisiones_tpp_agrupadas['co_corretaje_nombre'].str.lower().str.strip().eq('uib')) &
                    (comisiones_tpp_agrupadas['ComisionTPP'] > 0)
                ]
                
                comisiones_tpp_filtradas = comisiones_tpp_filtradas.rename(columns={'co_corretaje_nombre': 'vendedor', 'ComisionTPP': 'comision'})
                tpp_por_vendedor = comisiones_tpp_filtradas.to_dict(orient='records')

            # --- Chart Data Calculation (using uib which is equivalent to ComisionUIB) ---
            if not df_mes_actual.empty:
                ramos_data = df_mes_actual.groupby('ramo')['uib'].sum().sort_values(ascending=False).head(10)
                chart_data['labels'] = ramos_data.index.tolist()
                chart_data['data'] = [round(x) for x in ramos_data.values]
        else:
            flash(f'Faltan columnas requeridas en remisiones.xlsx para calcular el recaudo: {", ".join(missing_cols)}.', 'warning')

    return render_template('recaudo.html',
                           total_renovaciones=total_renovaciones,
                           renovaciones_data=renovaciones_data,
                           total_prospectos=total_prospectos,
                           prospectos_data=prospectos_data,
                           total_modificaciones=total_modificaciones,
                           modificaciones_data=modificaciones_data,
                           total_general=total_general,
                           tpp_por_vendedor=tpp_por_vendedor,
                           produccion_por_ramo_chart=chart_data)

@app.route('/visualizar_sarlaft', methods=['GET'])
@login_required
def visualizar_sarlaft():
    search_query = request.args.get('search_query', '').strip().lower()
    client_folders_path = app.config['CLIENT_FOLDERS_BASE_DIR']
    found_folders = []

    if os.path.exists(client_folders_path):
        if search_query:
            for folder_name in os.listdir(client_folders_path):
                if search_query in folder_name.lower():
                    found_folders.append(folder_name)

    return render_template('visualizar_sarlaft.html', folders=found_folders, search_query=search_query)

@app.route('/visualizar_sarlaft/<folder_name>')
@login_required
def visualizar_sarlaft_docs(folder_name):
    client_folder_path = os.path.join(app.config['CLIENT_FOLDERS_BASE_DIR'], folder_name)
    sarlaft_folder_path = os.path.join(client_folder_path, 'SARLAFT')
    found_docs = []

    if os.path.exists(sarlaft_folder_path):
        for year_folder in os.listdir(sarlaft_folder_path):
            year_folder_path = os.path.join(sarlaft_folder_path, year_folder)
            if os.path.isdir(year_folder_path):
                for doc_name in os.listdir(year_folder_path):
                    doc_name_lower = doc_name.lower()
                    # Check for both SARLAFT and Consulta Cliente documents
                    if 'sarlaft' in doc_name_lower or 'consulta_cliente_desqubra' in doc_name_lower:
                        found_docs.append({'year': year_folder, 'doc_name': doc_name})

    return render_template('visualizar_sarlaft_docs.html', folder_name=folder_name, found_docs=found_docs)

@app.route('/serve_sarlaft_doc/<folder_name>/<year>/<doc_name>')
@login_required
def serve_sarlaft_doc(folder_name, year, doc_name):
    file_path = os.path.join(app.config['CLIENT_FOLDERS_BASE_DIR'], folder_name, 'SARLAFT', year, doc_name)
    if os.path.exists(file_path):
        return send_file(file_path)
    else:
        return "Archivo no encontrado", 404

@app.route('/cobros/editar/<id_cobro>')
@login_required
def editar_cobro(id_cobro):
    cobro_data = None
    if os.path.exists(COBROS_FILE):
        try:
            df = pd.read_excel(COBROS_FILE)
            df['ID_COBRO'] = df['ID_COBRO'].astype(str)
            cobro_data = df[df['ID_COBRO'] == id_cobro].to_dict('records')
            if not cobro_data:
                flash('Error: No se encontró el cobro especificado.', 'danger')
                return redirect(url_for('panel_cobros'))
        except Exception as e:
            flash(f'Error al leer el archivo de cobros: {e}', 'danger')
            return redirect(url_for('panel_cobros'))

    return render_template('editar_cobro.html', cobro=cobro_data[0] if cobro_data else None)

@app.route('/cobros')
@login_required
def panel_cobros():
    try:
        # --- 1. Carga y Preparación de Datos ---
        if not os.path.exists(COBROS_FILE):
            flash('No se encontró el archivo de cobros.', 'warning')
            return render_template('cobros.html', 
                                   cobros_data={'records': [], 'kpis': {}, 'pagination': None, 'selected_period': 'Mensual'},
                                   pagos_data={'records': [], 'kpis': {}, 'pagination': None, 'selected_period': 'Mensual'},
                                   opciones_periodicidad=[])

        df = pd.read_excel(COBROS_FILE)
        df['Fecha_Vencimiento_Cuota'] = pd.to_datetime(df['Fecha_Vencimiento_Cuota'], errors='coerce')
        df.dropna(subset=['Fecha_Vencimiento_Cuota'], inplace=True)
        
        # --- Compatibilidad para registros antiguos ---
        if 'Periodicidad' not in df.columns: df['Periodicidad'] = 'Mensual'
        df['Periodicidad'].fillna('Mensual', inplace=True)
        if 'Tipo_Movimiento' not in df.columns: df['Tipo_Movimiento'] = 'Cobro'
        df['Tipo_Movimiento'].fillna('Cobro', inplace=True)

        opciones_periodicidad = config_manager.get_list('periodicidad_pago')
        hoy = datetime.now()

        # --- 2. Separar DataFrames ---
        df_cobros_raw = df[df['Tipo_Movimiento'].str.contains('Cobro', case=False, na=False)].copy()
        df_pagos_raw = df[df['Tipo_Movimiento'].str.contains('Pago', case=False, na=False)].copy()

        # --- 3. Función de procesamiento reutilizable ---
        def process_section(df_raw, section_name_prefix):
            kpis = {}
            primer_dia_mes_actual = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

            # --- DataFrames para cada categoría de filtro ---
            df_pendientes = df_raw[df_raw['Estado'] == 'Pendiente'].copy()
            
            # "Mensual": todos los pendientes del mes actual
            df_mensual_actual = df_pendientes[
                (df_pendientes['Fecha_Vencimiento_Cuota'].dt.month == hoy.month) & 
                (df_pendientes['Fecha_Vencimiento_Cuota'].dt.year == hoy.year)
            ]
            
            # "Futuros": todos los pendientes a partir de hoy
            df_futuro = df_pendientes[df_pendientes['Fecha_Vencimiento_Cuota'] >= hoy]
            
            # "Pasados": todos los pendientes antes del mes actual
            df_pasados = df_pendientes[df_pendientes['Fecha_Vencimiento_Cuota'] < primer_dia_mes_actual]

            # --- Lógica para KPIs ---
            kpis['Mensual'] = len(df_mensual_actual)
            kpis['Trimestral'] = len(df_futuro[df_futuro['Periodicidad'] == 'Trimestral'].drop_duplicates(subset=['Tomador', 'N_Poliza']))
            kpis['Anual'] = len(df_futuro[df_futuro['Periodicidad'] == 'Anual'].drop_duplicates(subset=['Tomador', 'N_Poliza']))
            kpis['Pendientes Mes Anterior'] = len(df_pasados)

            # --- Lógica para selección de tabla y paginación ---
            periodicidad_seleccionada = request.args.get(f'periodicidad_{section_name_prefix}', 'Mensual')
            page = request.args.get(f'page_{section_name_prefix}', 1, type=int)
            per_page = 15

            if periodicidad_seleccionada == 'Mensual':
                registros_filtrados_df = df_mensual_actual
            elif periodicidad_seleccionada == 'Pendientes Mes Anterior':
                registros_filtrados_df = df_pasados
            else: # Trimestral, Anual, etc.
                df_futuro_filtrado = df_futuro[df_futuro['Periodicidad'] == periodicidad_seleccionada]
                registros_filtrados_df = df_futuro_filtrado.sort_values('Fecha_Vencimiento_Cuota').drop_duplicates(subset=['Tomador', 'N_Poliza'], keep='first')

            total_registros = len(registros_filtrados_df)
            total_pages = (total_registros + per_page - 1) // per_page if per_page > 0 else 0
            
            start = (page - 1) * per_page
            end = start + per_page
            registros_paginados_df = registros_filtrados_df.iloc[start:end]

            records_list = registros_paginados_df.sort_values(by='Fecha_Vencimiento_Cuota').to_dict(orient='records')

            pagination_info = {
                'page': page, 'total_pages': total_pages, 'total_registros': total_registros,
                'has_prev': page > 1, 'has_next': page < total_pages,
                'prev_num': page - 1, 'next_num': page + 1,
                'param_prefix': section_name_prefix
            }
            
            return {
                'records': records_list, 'kpis': kpis, 'pagination': pagination_info,
                'selected_period': periodicidad_seleccionada
            }

        # --- 4. Procesar ambas secciones ---
        cobros_data = process_section(df_cobros_raw, 'cobros')
        pagos_data = process_section(df_pagos_raw, 'pagos')

    except Exception as e:
        flash(f"Error al procesar el panel de cobros: {e}", "danger")
        import traceback
        traceback.print_exc()
        return redirect(url_for('index'))

    return render_template('cobros.html', 
                           cobros_data=cobros_data,
                           pagos_data=pagos_data,
                           opciones_periodicidad=opciones_periodicidad)

@app.route('/marcar_cobrado/<id_cobro>', methods=['POST'])
@login_required
def marcar_cobrado(id_cobro):
    if os.path.exists(COBROS_FILE):
        try:
            df = pd.read_excel(COBROS_FILE)
            df['ID_COBRO'] = df['ID_COBRO'].astype(str)

            if id_cobro in df['ID_COBRO'].values:
                df.loc[df['ID_COBRO'] == id_cobro, 'Estado'] = 'Cobrado'
                df.to_excel(COBROS_FILE, index=False)
                flash('Cuota marcada como Cobrada.', 'success')
            else:
                flash('Error: No se encontró el ID del cobro.', 'danger')
        except Exception as e:
            flash(f'Error al actualizar el cobro: {e}', 'danger')
    else:
        flash('Error: Archivo de cobros no encontrado.', 'danger')

    return redirect(url_for('panel_cobros'))

if __name__ == '__main__':
    try:
        app.run(host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        import traceback
        with open('server_error.log', 'w') as f:
            f.write(str(e) + '\n')
            f.write(traceback.format_exc())