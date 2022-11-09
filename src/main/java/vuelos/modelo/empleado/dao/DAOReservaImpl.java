package vuelos.modelo.empleado.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.xdevapi.Result;

import vuelos.modelo.empleado.beans.AeropuertoBean;
import vuelos.modelo.empleado.beans.AeropuertoBeanImpl;
//import com.mysql.cj.xdevapi.Statement;
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
import vuelos.modelo.empleado.dao.datosprueba.DAOReservaDatosPrueba;
import vuelos.modelo.empleado.dao.DAOPasajeroImpl;
import vuelos.modelo.empleado.dao.DAOVuelosImpl;

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

		/**
		 * TODO (parte 2) Realizar una reserva de ida solamente llamando al Stored Procedure (S.P.) correspondiente. 
		 *      Si la reserva tuvo exito deberá retornar el número de reserva. Si la reserva no tuvo éxito o 
		 *      falla el S.P. deberá propagar un mensaje de error explicativo dentro de una excepción.
		 *      La demás excepciones generadas automáticamente por algun otro error simplemente se propagan.
		 *      
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOReservaImpl(...)).
		 *		
		 * 
		 * @throws Exception. Deberá propagar la excepción si ocurre alguna. Puede capturarla para loguear los errores
		 *		   pero luego deberá propagarla para que el controlador se encargue de manejarla.
		 *
		 * try (CallableStatement cstmt = conexion.prepareCall("CALL PROCEDURE reservaSoloIda(?, ?, ?, ?, ?, ?, ?)"))
		 * {
		 *  ...
		 * }
		 * catch (SQLException ex){
		 * 			logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		 *  		throw ex;
		 * } 
		 */
		
		/*
		 * Datos estaticos de prueba: Quitar y reemplazar por código que invoca al S.P.
		 * 
		 * - Si pasajero tiene nro_doc igual a 1 retorna 101 codigo de reserva y si se pregunta por dicha reserva como dato de prueba resultado "Reserva confirmada"
		 * - Si pasajero tiene nro_doc igual a 2 retorna 102 codigo de reserva y si se pregunta por dicha reserva como dato de prueba resultado "Reserva en espera"
		 * - Si pasajero tiene nro_doc igual a 3 se genera una excepción, resultado "No hay asientos disponibles"
		 * - Si pasajero tiene nro_doc igual a 4 se genera una excepción, resultado "El empleado no es válido"
		 * - Si pasajero tiene nro_doc igual a 5 se genera una excepción, resultado "El pasajero no está registrado"
		 * - Si pasajero tiene nro_doc igual a 6 se genera una excepción, resultado "El vuelo no es válido"
		 * - Si pasajero tiene nro_doc igual a 7 se genera una excepción de conexión.
		 */

		CallableStatement cstmnt = conexion.prepareCall("{CALL reservarSoloIda(?, ?, ?, ?, ?, ?, ?)}");
		Statement st = conexion.createStatement();
		int id_reserva;

		try {
			cstmnt.setInt(1, empleado.getLegajo());
			cstmnt.setString(2, pasajero.getTipoDocumento());
			cstmnt.setInt(3, pasajero.getNroDocumento());
			cstmnt.setString(4, vuelo.getNroVuelo());
			cstmnt.setDate(5, vuelo.getFechaVuelo());
			cstmnt.setString(6, detalleVuelo.getClase());
			cstmnt.registerOutParameter(7, java.sql.Types.VARCHAR);
			cstmnt.executeUpdate();
			
			String result = cstmnt.getString(7);
			ResultSet rs = (st.executeQuery("SELECT LAST_INSERT_ID() AS id_reserva"));
			rs.next();
			id_reserva = rs.getInt("id_reserva");
			ReservaBean r = recuperarReserva(id_reserva);
			
			logger.debug("Resultado: {}", result);
			logger.debug("Reserva: {}, {}", r.getNumero(), r.getEstado());
		
		}catch (SQLException ex){
				logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		 		throw ex;
		 } 

		 cstmnt.close();
		 st.close();

		/*
		DAOReservaDatosPrueba.registrarReservaSoloIda(pasajero, vuelo, detalleVuelo, empleado);
		ReservaBean r = DAOReservaDatosPrueba.getReserva();
		logger.debug("Reserva: {}, {}", r.getNumero(), r.getEstado());
		int resultado = DAOReservaDatosPrueba.getReserva().getNumero();
		*/

		return id_reserva;
		// Fin datos estáticos de prueba.
	}
	
	@Override
	public int reservarIdaVuelta(PasajeroBean pasajero, 
				 				 InstanciaVueloBean vueloIda,
				 				 DetalleVueloBean detalleVueloIda,
				 				 InstanciaVueloBean vueloVuelta,
				 				 DetalleVueloBean detalleVueloVuelta,
				 				 EmpleadoBean empleado) throws Exception {
		
		logger.info("Realiza la reserva de ida y vuelta con pasajero {}", pasajero.getNroDocumento());

		/**
		 * TODO (parte 2) Realizar una reserva de ida y vuelta llamando al Stored Procedure (S.P.) correspondiente. 
		 *      Si la reserva tuvo exito deberá retornar el número de reserva. Si la reserva no tuvo éxito o 
		 *      falla el S.P. deberá propagar un mensaje de error explicativo dentro de una excepción.
		 *      La demás excepciones generadas automáticamente por algun otro error simplemente se propagan.
		 *      
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOReservaImpl(...)).
		 * 
		 * @throws Exception. Deberá propagar la excepción si ocurre alguna. Puede capturarla para loguear los errores
		 *		   pero luego deberá propagarla para que se encargue el controlador.
		 *
		 * try (CallableStatement ... )
		 * {
		 *  ...
		 * }
		 * catch (SQLException ex){
		 * 			logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		 *  		throw ex;
		 * } 
		 */
		
		/*
		 * Datos státicos de prueba: Quitar y reemplazar por código que invoca al S.P.
		 * 
		 * - Si pasajero tiene nro_doc igual a 1 retorna 101 codigo de reserva y si se pregunta por dicha reserva como dato de prueba resultado "Reserva confirmada"
		 * - Si pasajero tiene nro_doc igual a 2 retorna 102 codigo de reserva y si se pregunta por dicha reserva como dato de prueba resultado "Reserva en espera"
		 * - Si pasajero tiene nro_doc igual a 3 se genera una excepción, resultado "No hay asientos disponibles"
		 * - Si pasajero tiene nro_doc igual a 4 se genera una excepción, resultado "El empleado no es válido"
		 * - Si pasajero tiene nro_doc igual a 5 se genera una excepción, resultado "El pasajero no está registrado"
		 * - Si pasajero tiene nro_doc igual a 6 se genera una excepción, resultado "El vuelo no es válido"
		 * - Si pasajero tiene nro_doc igual a 7 se genera una excepción de conexión.
		 */	

		CallableStatement cstmnt = conexion.prepareCall("{CALL reservarIdaVuelta(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
		Statement st = conexion.createStatement();
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
			cstmnt.executeUpdate();
			
			String result = cstmnt.getString(10);
			ResultSet rs = (st.executeQuery("SELECT LAST_INSERT_ID() AS id_reserva"));
			rs.next();
			id_reserva = rs.getInt("id_reserva");
			ReservaBean r = recuperarReserva(id_reserva);
			
			logger.debug("Resultado: {}", result);
			logger.debug("Reserva: {}, {}", r.getNumero(), r.getEstado());
		
		}catch (SQLException ex){
				logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		 		throw ex;
		 } 

		 cstmnt.close();
		 st.close();

		/*
		DAOReservaDatosPrueba.registrarReservaIdaVuelta(pasajero, vueloIda, detalleVueloIda, vueloVuelta, detalleVueloVuelta, empleado);
		int resultado = DAOReservaDatosPrueba.getReserva().getNumero();
		*/

		return id_reserva;
		// Fin datos estáticos de prueba.
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

		System.out.println(datos_reserva + " \n"); // QUITAR DESPUES DE TESTEAR
		System.out.println(datos_reserva_vuelos + " \n"); // QUITAR DESPUES DE TESTEAR
		/* Falta agregar las tuplas dinamicamente a la base de datos para que funcionen las queries, ya que sino es imposible 
		* obtener un RS con algo.
		* La version estatica del profe funciona porque hace un getReserva() que devuelve un bean de Reserva, pero no esta cargado
		* en la base de datos realmente.
		* (Fijarse que no se modifica nada en las cantidades de asientos reservados y demás)
		*/

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

				System.out.println("Seccion: Datos reserva.");

				DAOPasajero dp = new DAOPasajeroImpl(conexion);
				PasajeroBean p = dp.recuperarPasajero(rs_reserva.getString("pasajero_doc_tipo"), rs_reserva.getInt("pasajero_doc_nro"));
				reserva.setPasajero(p);

				DAOEmpleado de = new DAOEmpleadoImpl(conexion);
				EmpleadoBean e = de.recuperarEmpleado(rs_reserva.getInt("empleado_legajo"));
				reserva.setEmpleado(e);

				int count = 0;
				ArrayList<InstanciaVueloClaseBean> vuelos_clase = new ArrayList<InstanciaVueloClaseBean>();

				while (rs_reserva_vuelos.next()) {
					System.out.println("Seccion: Datos reserva-aviones.");
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
				} else reserva.setEsIdaVuelta(false);
				
				System.out.println("La cantidad de aviones recuperados es " + count + " \n");

				reserva.setVuelosClase(vuelos_clase);

				logger.debug("Se recuperó la reserva: {}, {}", reserva.getNumero(), reserva.getEstado());
			}

		} catch (SQLException ex){
			logger.error("SQLException: " + ex.getMessage());
            logger.error("SQLState: " + ex.getSQLState());
            logger.error("VendorError: " + ex.getErrorCode());
            throw new Exception("Error en la conexión con la BD.");
		}

		/**
		 * TODO (parte 2) Debe realizar una consulta que retorne un objeto ReservaBean donde tenga los datos de la
		 *      reserva que corresponda con el codigoReserva y en caso que no exista generar una excepción.
		 *
		 * 		Debe poblar la reserva con todas las instancias de vuelo asociadas a dicha reserva y 
		 * 		las clases correspondientes.
		 * 
		 * 		Los objetos ReservaBean además de las propiedades propias de una reserva tienen un arraylist
		 * 		con pares de instanciaVuelo y Clase. Si la reserva es solo de ida va a tener un unico
		 * 		elemento el arreglo, y si es de ida y vuelta tendrá dos elementos. 
		 * 
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOReservaImpl(...)).
		 */
		/*
		 * Importante, tenga en cuenta de setear correctamente el atributo IdaVuelta con el método setEsIdaVuelta en la ReservaBean
		 */
		// Datos estáticos de prueba. Quitar y reemplazar por código que recupera los datos reales.

		return reserva;
	}

}
