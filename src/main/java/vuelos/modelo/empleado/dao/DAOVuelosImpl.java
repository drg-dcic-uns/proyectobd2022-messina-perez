package vuelos.modelo.empleado.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.sql.Statement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vuelos.modelo.empleado.beans.AeropuertoBean;
import vuelos.modelo.empleado.beans.AeropuertoBeanImpl;
import vuelos.modelo.empleado.beans.DetalleVueloBean;
import vuelos.modelo.empleado.beans.DetalleVueloBeanImpl;
import vuelos.modelo.empleado.beans.InstanciaVueloBean;
import vuelos.modelo.empleado.beans.InstanciaVueloBeanImpl;
import vuelos.modelo.empleado.beans.UbicacionesBean;
import vuelos.modelo.empleado.beans.UbicacionesBeanImpl;
import vuelos.modelo.empleado.dao.datosprueba.DAOVuelosDatosPrueba;

import vuelos.utils.Fechas;
import vuelos.utils.Parsing;

public class DAOVuelosImpl implements DAOVuelos {

	private static Logger logger = LoggerFactory.getLogger(DAOVuelosImpl.class);
	
	//conexión para acceder a la Base de Datos
	private Connection conexion;
	
	public DAOVuelosImpl(Connection conexion) {
		this.conexion = conexion;
	}

	@Override
	public ArrayList<InstanciaVueloBean> recuperarVuelosDisponibles(Date fechaVuelo, UbicacionesBean origen, UbicacionesBean destino)  throws Exception {
		/** 
		 * TODO Debe retornar una lista de vuelos disponibles para ese día con origen y destino según los parámetros. 
		 *      Debe propagar una excepción si hay algún error en la consulta.    
		 *      
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOVuelosImpl(...)).  
		 */
		//Datos estáticos de prueba. Quitar y reemplazar por código que recupera los datos reales.
		String paisOrigen = origen.getPais();
		String estadoOrigen = origen.getEstado();
		String ciudadOrigen = origen.getCiudad();
		String paisDestino = destino.getPais();
		String estadoDestino = destino.getEstado();
		String ciudadDestino = destino.getCiudad();
		ArrayList<InstanciaVueloBean> resultado = new ArrayList<InstanciaVueloBean>();
		String sql = "SELECT DISTINCT nro_vuelo, modelo, fecha, dia_sale, hora_sale, hora_llega, tiempo_estimado, codigo_aero_sale, codigo_aero_llega, nombre_aero_sale, nombre_aero_llega, vs.direccion as 'dir_sale', vs.telefono as 'tel_sale', vl.direccion as 'dir_llega', vl.telefono as 'tel_llega' "; 
		        sql = sql + "FROM vuelos_disponibles join aeropuertos as vs on codigo_aero_sale = vs.codigo join aeropuertos as vl on codigo_aero_llega = vl.codigo";
                sql = sql + " WHERE vs.ciudad = '"+ ciudadOrigen + "' and vs.pais = '"+ paisOrigen + "' and vs.estado = '"+ estadoOrigen + "'";
                sql = sql + " and vl.ciudad = '"+ ciudadDestino + "' and vl.pais = '"+ paisDestino + "' and vl.estado = '"+ estadoDestino + "'";
                sql = sql + " and fecha= '"+ Fechas.convertirDateAStringDB(fechaVuelo) +"'";

		logger.debug("SQL: {}", sql);
		try{ 
			 Statement select = conexion.createStatement();
			 ResultSet rs = select.executeQuery(sql);
			
			 while (rs.next()) {
				//logger.debug("Se recuperÃ³ el item con nombre {} y fecha {}", rs.getString("nombre_batalla"), rs.getDate("fecha"));
				InstanciaVueloBean instancia = new InstanciaVueloBeanImpl(); 	
				instancia.setNroVuelo(rs.getString("nro_vuelo"));
				instancia.setModelo(rs.getString("modelo"));	
				instancia.setFechaVuelo(rs.getDate("fecha"));
				instancia.setDiaSalida(rs.getString("dia_sale"));
				instancia.setHoraSalida(rs.getTime("hora_sale"));
				instancia.setHoraLlegada(rs.getTime("hora_llega"));
				instancia.setTiempoEstimado(rs.getTime("tiempo_estimado"));		
				
				AeropuertoBean salida = new AeropuertoBeanImpl();
				salida.setCodigo(rs.getString("codigo_aero_sale"));
				salida.setDireccion(rs.getString("dir_sale"));
				salida.setNombre(rs.getString("nombre_aero_sale"));
				salida.setTelefono(rs.getString("tel_sale"));
				salida.setUbicacion(origen);
				instancia.setAeropuertoSalida(salida);
				
				AeropuertoBean llegada = new AeropuertoBeanImpl();
				llegada.setCodigo(rs.getString("codigo_aero_llega"));
				llegada.setDireccion(rs.getString("dir_llega"));
				llegada.setNombre(rs.getString("nombre_aero_llega"));
				llegada.setTelefono(rs.getString("tel_llega"));
				llegada.setUbicacion(destino);
				instancia.setAeropuertoLlegada(llegada);	
				
				resultado.add(instancia);			
			  }
			 
			 rs.close();
			 select.close();
				
		}
		catch (SQLException ex)
		{			
			logger.error("SQLException: " + ex.getMessage());
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());
			throw new Exception("Error inesperado al consultar la B.D.");
		}
		
		return resultado;
	}

	@Override
	public ArrayList<DetalleVueloBean> recuperarDetalleVuelo(InstanciaVueloBean vuelo) throws Exception {
		/** 
		 * TODO Debe retornar una lista de clases, precios y asientos disponibles de dicho vuelo.		   
		 *      Debe propagar una excepción si hay algún error en la consulta.    
		 *      
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOVuelosImpl(...)).
		 */
		//Datos estáticos de prueba. Quitar y reemplazar por código que recupera los datos reales.
		ArrayList<DetalleVueloBean> resultado = new ArrayList<DetalleVueloBean>();
		
		String sql = "SELECT clase, precio, asientos_disponibles FROM vuelos_disponibles WHERE nro_vuelo = '" + vuelo.getNroVuelo() + "' and fecha = '" + Fechas.convertirDateAStringDB(vuelo.getFechaVuelo()) + "'";
		Statement select = conexion.createStatement();
		ResultSet rs = select.executeQuery(sql);

		while(rs.next()) {
			DetalleVueloBean dv = new DetalleVueloBeanImpl();

			dv.setVuelo(vuelo);
			dv.setClase(rs.getString("clase"));
			dv.setPrecio((float) Parsing.parseMonto(rs.getString("precio")));
			dv.setAsientosDisponibles(rs.getInt("asientos_disponibles"));

			resultado.add(dv);
		}

		rs.close();
		select.close();

		return resultado; 
	}
}
