package vuelos.modelo.empleado;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vuelos.modelo.ModeloImpl;
import vuelos.modelo.empleado.beans.DetalleVueloBean;
import vuelos.modelo.empleado.beans.EmpleadoBean;
import vuelos.modelo.empleado.beans.InstanciaVueloBean;
import vuelos.modelo.empleado.beans.PasajeroBean;
import vuelos.modelo.empleado.beans.ReservaBean;
import vuelos.modelo.empleado.beans.UbicacionesBean;
import vuelos.modelo.empleado.beans.UbicacionesBeanImpl;
import vuelos.modelo.empleado.dao.DAOEmpleado;
import vuelos.modelo.empleado.dao.DAOEmpleadoImpl;
import vuelos.modelo.empleado.dao.DAOPasajero;
import vuelos.modelo.empleado.dao.DAOPasajeroImpl;
import vuelos.modelo.empleado.dao.DAOReserva;
import vuelos.modelo.empleado.dao.DAOReservaImpl;
import vuelos.modelo.empleado.dao.DAOVuelos;
import vuelos.modelo.empleado.dao.DAOVuelosImpl;
import vuelos.modelo.empleado.dao.datosprueba.DAOUbicacionesDatosPrueba;
import vuelos.utils.Conexion;

public class ModeloEmpleadoImpl extends ModeloImpl implements ModeloEmpleado {

	private static Logger logger = LoggerFactory.getLogger(ModeloEmpleadoImpl.class);	

	
	private Integer legajo = null;
	
	public ModeloEmpleadoImpl() throws Exception {
		// inicializa la conexión con el servidor utilizando el método estático de la clase Conexion
		// que setea el driver (jdbc), la ubicación del servidor y puerto (url) y la bases de datos, 
		// recuperando estos datos de un archivo de propiedades  
		try
		{
			Conexion.inicializar("cfg/conexionBD.properties");
		}
		catch  (Exception ex) {
			logger.error("Error al recuperar el archivo de propiedades de la BD y no se pudo establecer la conexion");
			throw new Exception("No se pudo conectar al servidor. Error al recuperar el archivo de configuración de la B.D.");
		}
	}
	

	@Override
	public boolean autenticarUsuarioAplicacion(String legajo, String password) throws Exception {
		boolean resultado = false;
		logger.info("Se intenta autenticar el legajo {} con password {}", legajo, password);

		String query = "SELECT * FROM empleados WHERE legajo = '"+ legajo +"' and password = md5('" + password +"')";
		ResultSet rs = this.consulta(query);

		if (rs.next()) {
			resultado = true;
			this.legajo = Integer.parseInt(legajo);
		} else {
			resultado = false;
		}

		java.sql.Statement temp = rs.getStatement();
		rs.close();
		temp.close();
		
		return resultado;
	}
	
	@Override
	public ArrayList<String> obtenerTiposDocumento() {
		logger.info("recupera los tipos de documentos.");
		/** 
		 * TODO Debe retornar una lista de strings con los tipos de documentos. 
		 *      Deberia propagar una excepción si hay algún error en la consulta.
		 */
		
		/*
		 * Datos estáticos de prueba. Quitar y reemplazar por código que recupera los datos reales. 
		 * 
		 *  Como no hay una tabla con los tipos de documento, se deberán recuperar todos los tipos validos
		 *  de la tabla de pasajeros
		 */
		ArrayList<String> tipos = new ArrayList<String>();
		tipos.add("DNI");
		tipos.add("Pasaporte");
		// Fin datos estáticos de prueba.
		
		return tipos;
	}		
	
	@Override
	public EmpleadoBean obtenerEmpleadoLogueado() throws Exception {
		logger.info("Solicita al DAO un empleado con legajo {}", this.legajo);
		if (this.legajo == null) {
			logger.info("No hay un empleado logueado.");
			throw new Exception("No hay un empleado logueado. La sesión terminó.");
		}
		
		DAOEmpleado dao = new DAOEmpleadoImpl(this.conexion);
		return dao.recuperarEmpleado(this.legajo);
	}	

	@Override
	public ArrayList<UbicacionesBean> recuperarUbicaciones() throws Exception {
		logger.info("recupera las ciudades que tienen aeropuertos.");
        ArrayList<UbicacionesBean> lista = null;
        /** 
         * TODO Debe retornar una lista de UbicacionesBean con todas las ubicaciones almacenadas en la B.D. 
         *      Deberia propagar una excepción si hay algún error en la consulta.
         *
         *      Reemplazar el siguiente código de prueba por los datos obtenidos desde la BD.
         */
        try{
            lista = new ArrayList<UbicacionesBean>();
            ResultSet rs= this.consulta("select pais, estado, ciudad, huso from vuelos.ubicaciones");
            while (rs.next()) {
                UbicacionesBean ubicacion= new UbicacionesBeanImpl();
                ubicacion.setCiudad(rs.getString("ciudad"));
                ubicacion.setEstado(rs.getString("estado"));
                ubicacion.setHuso(rs.getInt("huso"));
                ubicacion.setPais(rs.getString("pais"));
                lista.add(ubicacion);
            }
       }
       catch (SQLException ex) {
           logger.error("SQLException: " + ex.getMessage());
           logger.error("SQLState: " + ex.getSQLState());
           logger.error("VendorError: " + ex.getErrorCode());
           throw new Exception("Error en la conexión con la BD.");
       }
        return lista;
	}


	@Override
	public ArrayList<InstanciaVueloBean> obtenerVuelosDisponibles(Date fechaVuelo, UbicacionesBean origen, UbicacionesBean destino) throws Exception {
		
		logger.info("Recupera la lista de vuelos disponibles para la fecha {} desde {} a {}.", fechaVuelo, origen, destino);

		DAOVuelos dao = new DAOVuelosImpl(this.conexion);		
		return dao.recuperarVuelosDisponibles(fechaVuelo, origen, destino);
	}
	
	@Override
	public ArrayList<DetalleVueloBean> obtenerDetalleVuelo(InstanciaVueloBean vuelo) throws Exception {

		logger.info("Recupera la cantidad de asientos y precio del vuelo {} .", vuelo.getNroVuelo());
		
		DAOVuelos dao = new DAOVuelosImpl(this.conexion);		
		return dao.recuperarDetalleVuelo(vuelo);
	}


	@Override
	public PasajeroBean obtenerPasajero(String tipoDoc, int nroDoc) throws Exception {
		logger.info("Solicita al DAO un pasajero con tipo {} y nro {}", tipoDoc, nroDoc);
		
		DAOPasajero dao = new DAOPasajeroImpl(this.conexion);
		return dao.recuperarPasajero(tipoDoc, nroDoc);
	}


	@Override
	public ReservaBean reservarSoloIda(PasajeroBean pasajero, InstanciaVueloBean vuelo, DetalleVueloBean detalleVuelo)
			throws Exception {
		logger.info("Se solicita al modelo realizar una reserva solo ida");

		EmpleadoBean empleadoLogueado = this.obtenerEmpleadoLogueado();
		
		DAOReserva dao = new DAOReservaImpl(this.conexion);
		int nroReserva = dao.reservarSoloIda(pasajero, vuelo, detalleVuelo, empleadoLogueado);
		
		ReservaBean reserva = dao.recuperarReserva(nroReserva); 
		return reserva;
	}


	@Override
	public ReservaBean reservarIdaVuelta(PasajeroBean pasajeroSeleccionado, 
									 InstanciaVueloBean vueloIdaSeleccionado,
									 DetalleVueloBean detalleVueloIdaSeleccionado, 
									 InstanciaVueloBean vueloVueltaSeleccionado,
									 DetalleVueloBean detalleVueloVueltaSeleccionado) throws Exception {
		
		logger.info("Se solicita al modelo realizar una reserva de ida y vuelta");
		
		EmpleadoBean empleadoLogueado = this.obtenerEmpleadoLogueado();
		
		DAOReserva dao = new DAOReservaImpl(this.conexion);
		
		int nroReserva = dao.reservarIdaVuelta(pasajeroSeleccionado, 
									 vueloIdaSeleccionado, 
									 detalleVueloIdaSeleccionado, 
									 vueloVueltaSeleccionado, 
									 detalleVueloVueltaSeleccionado, 
									 empleadoLogueado);
		
		ReservaBean reserva = dao.recuperarReserva(nroReserva); 
		return reserva;		
	}
}
