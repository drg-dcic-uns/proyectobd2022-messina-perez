package vuelos.modelo.empleado.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vuelos.modelo.empleado.beans.AeropuertoBean;
import vuelos.modelo.empleado.beans.AeropuertoBeanImpl;
import vuelos.modelo.empleado.beans.DetalleVueloBean;
import vuelos.modelo.empleado.beans.DetalleVueloBeanImpl;
import vuelos.modelo.empleado.beans.EmpleadoBean;
import vuelos.modelo.empleado.beans.InstanciaVueloBean;
import vuelos.modelo.empleado.beans.InstanciaVueloBeanImpl;
import vuelos.modelo.empleado.beans.InstanciaVueloClaseBean;
import vuelos.modelo.empleado.beans.InstanciaVueloClaseBeanImpl;
import vuelos.modelo.empleado.beans.PasajeroBean;
import vuelos.modelo.empleado.beans.ReservaBean;
import vuelos.modelo.empleado.beans.ReservaBeanImpl;
import vuelos.modelo.empleado.beans.UbicacionesBean;
import vuelos.modelo.empleado.beans.UbicacionesBeanImpl;

public class DAOReservaImpl implements DAOReserva {

	private static Logger logger = LoggerFactory.getLogger(DAOReservaImpl.class);
	
	//conexión para acceder a la Base de Datos
	private Connection conexion;
	
	public DAOReservaImpl(Connection conexion) {
		this.conexion = conexion;
	}
		
	
	@Override
	public int reservarSoloIda(PasajeroBean pasajero, 
							   InstanciaVueloBean vuelo, 
							   DetalleVueloBean detalleVuelo,
							   EmpleadoBean empleado) throws Exception {
		logger.info("Realiza la reserva de solo ida con pasajero {}", pasajero.getNroDocumento());

		CallableStatement cstmnt = conexion.prepareCall("{CALL reservarSoloIda(?, ?, ?, ?, ?, ?, ?, ?)}");
		Statement st = conexion.createStatement();
		String result;
		int id_reserva;

		try {
			cstmnt.setInt(1, empleado.getLegajo());
			cstmnt.setString(2, pasajero.getTipoDocumento());
			cstmnt.setInt(3, pasajero.getNroDocumento());
			cstmnt.setString(4, vuelo.getNroVuelo());
			cstmnt.setDate(5, vuelo.getFechaVuelo());
			cstmnt.setString(6, detalleVuelo.getClase());
			cstmnt.registerOutParameter(7, java.sql.Types.VARCHAR);
			cstmnt.registerOutParameter(8, java.sql.Types.INTEGER);
			cstmnt.executeUpdate();

			result = cstmnt.getString(7);
			id_reserva = cstmnt.getInt(8);
			
			logger.debug("Resultado: {}", result);
			
			if (id_reserva == -1) {
				throw new Exception(result);
			}
			
			ReservaBean r = recuperarReserva(id_reserva);
			logger.debug("Reserva: {}, {}", r.getNumero(), r.getEstado());
		
		}catch (SQLException ex){
				logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		 		throw ex;
		 } 

		 cstmnt.close();
		 st.close();

		return id_reserva;
	}
	
	@Override
	public int reservarIdaVuelta(PasajeroBean pasajero, 
				 				 InstanciaVueloBean vueloIda,
				 				 DetalleVueloBean detalleVueloIda,
				 				 InstanciaVueloBean vueloVuelta,
				 				 DetalleVueloBean detalleVueloVuelta,
				 				 EmpleadoBean empleado) throws Exception {
		
		logger.info("Realiza la reserva de ida y vuelta con pasajero {}", pasajero.getNroDocumento());

		CallableStatement cstmnt = conexion.prepareCall("{CALL reservarIdaVuelta(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
		String result;
		int id_reserva;

		try {
			cstmnt.setInt(1, empleado.getLegajo());
			cstmnt.setString(2, pasajero.getTipoDocumento());
			cstmnt.setInt(3, pasajero.getNroDocumento());
			cstmnt.setString(4, vueloIda.getNroVuelo());
			cstmnt.setDate(5, vueloIda.getFechaVuelo());
			cstmnt.setString(6, detalleVueloIda.getClase());
			cstmnt.setString(7, vueloVuelta.getNroVuelo());
			cstmnt.setDate(8, vueloVuelta.getFechaVuelo());
			cstmnt.setString(9, detalleVueloVuelta.getClase());
			cstmnt.registerOutParameter(10, java.sql.Types.VARCHAR);
			cstmnt.registerOutParameter(11, java.sql.Types.INTEGER);
			cstmnt.executeUpdate();
			
			result = cstmnt.getString(10);
			id_reserva = cstmnt.getInt(11);

			logger.debug("Resultado: {}", result);

			if (id_reserva == -1) {
				throw new Exception(result);
			}

			ReservaBean r = recuperarReserva(id_reserva);
			
			logger.debug("Reserva: {}, {}", r.getNumero(), r.getEstado());
		
		}catch (SQLException ex){
				logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		 		throw ex;
		 } 

		 cstmnt.close();

		return id_reserva;
	}
	
	@Override
	public ReservaBean recuperarReserva(int codigoReserva) throws Exception {
		logger.info("Solicita recuperar información de la reserva con codigo {}", codigoReserva);
		
		ReservaBean reserva = null;

		String datos_reserva = new String();
		datos_reserva += "SELECT ";
		datos_reserva += "r.numero AS 'nro_reserva', ";
		datos_reserva += "r.fecha AS 'fecha_reserva', ";
		datos_reserva += "r.vencimiento AS 'vencimiento_reserva', ";
		datos_reserva += "r.estado AS 'estado_reserva',";
		datos_reserva += "r.doc_tipo AS 'pasajero_doc_tipo', ";
		datos_reserva += "r.doc_nro AS 'pasajero_doc_nro', ";
		datos_reserva += "r.legajo AS 'empleado_legajo' ";
		datos_reserva += "FROM reservas r ";
		datos_reserva += "WHERE r.numero = " + codigoReserva + ";";

		String datos_reserva_vuelos = new String();
		datos_reserva_vuelos += "SELECT ";
		datos_reserva_vuelos += "vd.nro_vuelo, ";
		datos_reserva_vuelos += "vd.modelo, ";
		datos_reserva_vuelos += "vd.dia_sale, ";
		datos_reserva_vuelos += "vd.hora_sale, ";
		datos_reserva_vuelos += "vd.hora_llega, ";
		datos_reserva_vuelos += "vd.tiempo_estimado, ";
		datos_reserva_vuelos += "vd.fecha AS 'fecha_vuelo', ";
		datos_reserva_vuelos += "vd.codigo_aero_sale, ";
		datos_reserva_vuelos += "vd.nombre_aero_sale, ";
		datos_reserva_vuelos += "a_sale.telefono AS 'telefono_aero_sale', ";
		datos_reserva_vuelos += "a_sale.direccion AS 'direccion_aero_sale', ";
		datos_reserva_vuelos += "vd.pais_sale, ";
		datos_reserva_vuelos += "vd.estado_sale, ";
		datos_reserva_vuelos += "vd.ciudad_sale, ";
		datos_reserva_vuelos += "u_sale.huso AS 'huso_sale', ";
		datos_reserva_vuelos += "vd.codigo_aero_llega, ";
		datos_reserva_vuelos += "vd.nombre_aero_llega, ";
		datos_reserva_vuelos += "a_llega.telefono AS 'telefono_aero_llega', ";
		datos_reserva_vuelos += "a_llega.direccion AS 'direccion_aero_llega', ";
		datos_reserva_vuelos += "vd.pais_llega, ";
		datos_reserva_vuelos += "vd.estado_llega, ";
		datos_reserva_vuelos += "vd.ciudad_llega, ";
		datos_reserva_vuelos += "u_llega.huso AS 'huso_llega', ";
		datos_reserva_vuelos += "vd.clase, ";
		datos_reserva_vuelos += "vd.precio, ";
		datos_reserva_vuelos += "vd.asientos_disponibles ";
		datos_reserva_vuelos += "FROM ";
		datos_reserva_vuelos += "(reservas r ";
		datos_reserva_vuelos += "NATURAL JOIN reserva_vuelo_clase rvc ";
		datos_reserva_vuelos += "JOIN vuelos_disponibles vd ON (rvc.vuelo = vd.nro_vuelo AND rvc.fecha_vuelo = vd.fecha AND rvc.clase = vd.clase) ";
		datos_reserva_vuelos += "JOIN aeropuertos a_sale ON (vd.codigo_aero_sale = a_sale.codigo AND vd.nombre_aero_sale = a_sale.nombre) ";
		datos_reserva_vuelos += "JOIN ubicaciones u_sale ON (vd.ciudad_sale = u_sale.ciudad AND vd.estado_sale = u_sale.estado AND vd.pais_sale = u_sale.pais) ";
		datos_reserva_vuelos += "JOIN aeropuertos a_llega ON (vd.codigo_aero_llega = a_llega.codigo AND vd.nombre_aero_llega = a_llega.nombre) ";
		datos_reserva_vuelos += "JOIN ubicaciones u_llega ON (vd.ciudad_llega = u_llega.ciudad AND vd.estado_llega = u_llega.estado AND vd.pais_llega = u_llega.pais)) ";
		datos_reserva_vuelos += "WHERE r.numero = " + codigoReserva + ";";

		try {
			Statement st_reserva = conexion.createStatement();
			ResultSet rs_reserva = st_reserva.executeQuery(datos_reserva);
			Statement st_reserva_vuelos = conexion.createStatement();
			ResultSet rs_reserva_vuelos = st_reserva_vuelos.executeQuery(datos_reserva_vuelos);

			if (rs_reserva.next()) {
				reserva = new ReservaBeanImpl();
				reserva.setNumero(rs_reserva.getInt("nro_reserva"));
				reserva.setFecha(rs_reserva.getDate("fecha_reserva"));
				reserva.setVencimiento(rs_reserva.getDate("vencimiento_reserva"));
				reserva.setEstado(rs_reserva.getString("estado_reserva"));

				DAOPasajero dp = new DAOPasajeroImpl(conexion);
				PasajeroBean p = dp.recuperarPasajero(rs_reserva.getString("pasajero_doc_tipo"), rs_reserva.getInt("pasajero_doc_nro"));
				reserva.setPasajero(p);

				DAOEmpleado de = new DAOEmpleadoImpl(conexion);
				EmpleadoBean e = de.recuperarEmpleado(rs_reserva.getInt("empleado_legajo"));
				reserva.setEmpleado(e);

				int count = 0;
				ArrayList<InstanciaVueloClaseBean> vuelos_clase = new ArrayList<InstanciaVueloClaseBean>();

				while (rs_reserva_vuelos.next()) {
					count ++;

					// SE CREA LA NUEVA INSTANCIA VUELO_CLASE_BEAN.
					InstanciaVueloClaseBean ivc = new InstanciaVueloClaseBeanImpl();

					// ACCIONES ASOCIADAS AL SET DE INSTANCIA_VUELO_BEAN.

					InstanciaVueloBean iv = new InstanciaVueloBeanImpl();
					iv.setNroVuelo(rs_reserva_vuelos.getString("nro_vuelo"));
					iv.setModelo(rs_reserva_vuelos.getString("modelo"));
					iv.setDiaSalida(rs_reserva_vuelos.getString("dia_sale"));
					iv.setHoraSalida(rs_reserva_vuelos.getTime("hora_sale"));
					iv.setHoraLlegada(rs_reserva_vuelos.getTime("hora_llega"));
					iv.setTiempoEstimado(rs_reserva_vuelos.getTime("tiempo_estimado"));
					iv.setFechaVuelo(rs_reserva_vuelos.getDate("fecha_vuelo"));

					AeropuertoBean a_sale = new AeropuertoBeanImpl();
					a_sale.setCodigo(rs_reserva_vuelos.getString("codigo_aero_sale"));
					a_sale.setNombre(rs_reserva_vuelos.getString("nombre_aero_sale"));
					a_sale.setTelefono(rs_reserva_vuelos.getString("telefono_aero_sale"));
					a_sale.setDireccion(rs_reserva_vuelos.getString("direccion_aero_sale"));

					UbicacionesBean u_sale = new UbicacionesBeanImpl();
					u_sale.setPais(rs_reserva_vuelos.getString("pais_sale"));
					u_sale.setEstado(rs_reserva_vuelos.getString("estado_sale"));
					u_sale.setCiudad(rs_reserva_vuelos.getString("ciudad_sale"));
					u_sale.setHuso(rs_reserva_vuelos.getInt("huso_sale"));

					a_sale.setUbicacion(u_sale);
					iv.setAeropuertoSalida(a_sale);

					AeropuertoBean a_llega = new AeropuertoBeanImpl();
					a_llega.setCodigo(rs_reserva_vuelos.getString("codigo_aero_llega"));
					a_llega.setNombre(rs_reserva_vuelos.getString("nombre_aero_llega"));
					a_llega.setTelefono(rs_reserva_vuelos.getString("telefono_aero_llega"));
					a_llega.setDireccion(rs_reserva_vuelos.getString("direccion_aero_llega"));

					UbicacionesBean u_llega = new UbicacionesBeanImpl();
					u_llega.setPais(rs_reserva_vuelos.getString("pais_llega"));
					u_llega.setEstado(rs_reserva_vuelos.getString("estado_llega"));
					u_llega.setCiudad(rs_reserva_vuelos.getString("ciudad_llega"));
					u_llega.setHuso(rs_reserva_vuelos.getInt("huso_llega"));

					a_llega.setUbicacion(u_sale);
					iv.setAeropuertoLlegada(a_llega);

					ivc.setVuelo(iv);

					// ACCIONES ASOCIADAS AL SET DE DETALLE_VUELO_BEAN

					DetalleVueloBean dv = new DetalleVueloBeanImpl();
					dv.setVuelo(iv);
					dv.setClase(rs_reserva_vuelos.getString("clase"));
					dv.setPrecio(rs_reserva_vuelos.getFloat("precio"));
					dv.setAsientosDisponibles(rs_reserva_vuelos.getInt("asientos_disponibles"));

					ivc.setClase(dv);

					// SE AGREGA LA NUEVA INSTANCIA VUELO_CLASE_BEAN AL ARREGLO DE VUELO_CLASE DE LA RESERVA.

					vuelos_clase.add(ivc);
				}
				
				if (count == 2) {
					reserva.setEsIdaVuelta(true);
					logger.debug("reserva: esIdaVuelta {}", true);
				} else {
					reserva.setEsIdaVuelta(false);
					logger.debug("reserva: esIdaVuelta {}", false);
				}
				
				reserva.setVuelosClase(vuelos_clase);

				logger.debug("Se recuperó la reserva: {}, {}", reserva.getNumero(), reserva.getEstado());

				rs_reserva.close();
				st_reserva.close();
				rs_reserva_vuelos.close();
				st_reserva_vuelos.close();

			} else { // En caso de que no exista reserva con el codigo ingresado, se genera una excepción.
				throw new Exception("No existe reserva con codigo " + codigoReserva);
			}

		} catch (SQLException ex){
			logger.error("SQLException: " + ex.getMessage());
            logger.error("SQLState: " + ex.getSQLState());
            logger.error("VendorError: " + ex.getErrorCode());
            throw new Exception("Error en la conexión con la BD.");
		}

		return reserva;
	}

}
