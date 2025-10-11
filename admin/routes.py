import os
from functools import wraps
from flask import Blueprint, render_template, request, redirect, url_for, session, flash
import config_manager

# Definir el Blueprint para el panel de administración
admin_bp = Blueprint(
    'admin',
    __name__,
    template_folder='templates', # Apuntar al directorio de plantillas principal
    url_prefix='/admin'
)

# --- Decorador de autenticación ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'admin_logged_in' not in session:
            flash('Por favor, inicie sesión para acceder a esta página.', 'warning')
            return redirect(url_for('admin.login'))
        return f(*args, **kwargs)
    return decorated_function

# --- Rutas de autenticación ---
@admin_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # La contraseña se obtiene de una variable de entorno para seguridad.
        # El valor por defecto 'AdminUIB' se usa solo para desarrollo si la variable no está configurada.
        admin_password = os.environ.get('ADMIN_PASSWORD', 'AdminUIB')
        password = request.form.get('password')

        if password == admin_password:
            session['admin_logged_in'] = True
            flash('Inicio de sesión exitoso.', 'success')
            return redirect(url_for('admin.dashboard'))
        else:
            flash('Contraseña incorrecta.', 'danger')
    return render_template('admin/login.html')

@admin_bp.route('/logout')
@admin_required
def logout():
    session.pop('admin_logged_in', None)
    flash('Ha cerrado sesión.', 'info')
    return redirect(url_for('admin.login'))

# --- Rutas del panel de administración ---
@admin_bp.route('/')
@admin_required
def dashboard():
    return render_template('admin/dashboard.html')

@admin_bp.route('/listas')
@admin_required
def listas():
    list_names = config_manager.get_all_list_names()
    return render_template('admin/listas.html', list_names=list_names)

@admin_bp.route('/listas/editar/<list_name>', methods=['GET', 'POST'])
@admin_required
def edit_list_item(list_name):
    """
    Ruta unificada para editar y guardar cualquier lista de configuración.
    Despacha a la plantilla correcta y maneja la lógica de guardado
    basado en el nombre de la lista.
    """
    if request.method == 'POST':
        # Guardar los datos
        if list_name == 'vendedores':
            items = []
            nombres = request.form.getlist('item_nombre')
            comisiones = request.form.getlist('item_comision')
            for nombre, comision in zip(nombres, comisiones):
                if nombre.strip():
                    items.append({'nombre': nombre.strip(), 'comision': comision.strip() or '0'})
        else:
            # Manejar la lista de ítems de la tabla
            items = request.form.getlist('item')
            # Filtrar valores vacíos para que puedan ser eliminados al guardar
            items = [item.strip() for item in items if item.strip()]

        if config_manager.save_list(list_name, items):
            flash(f"Lista '{list_name}' guardada con éxito.", 'success')
        else:
            flash(f"Error al guardar la lista '{list_name}'.", 'danger')

        return redirect(url_for('admin.edit_list_item', list_name=list_name))

    # Método GET: Mostrar el formulario de edición
    items = config_manager.get_list(list_name)
    if items is None:
        flash(f"La lista '{list_name}' no fue encontrada.", 'danger')
        return redirect(url_for('admin.listas'))

    if list_name == 'vendedores':
        # Renderizar la plantilla específica para vendedores
        return render_template('admin/editar_vendedores.html', list_name=list_name, items=items)
    else:
        # Renderizar la plantilla genérica para listas simples
        return render_template('admin/editar_lista_simple.html', list_name=list_name, items=items)