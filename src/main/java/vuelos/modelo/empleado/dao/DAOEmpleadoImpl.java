package vuelos.modelo.empleado.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.protocol.Resultset;

import vuelos.modelo.empleado.beans.EmpleadoBean;
import vuelos.modelo.empleado.beans.EmpleadoBeanImpl;

public class DAOEmpleadoImpl implements DAOEmpleado {

	private static Logger logger = LoggerFactory.getLogger(DAOEmpleadoImpl.class);
	
	//conexión para acceder a la Base de Datos
	private Connection conexion;
	
	public DAOEmpleadoImpl(Connection c) {
		this.conexion = c;
	}


	@Override
	public EmpleadoBean recuperarEmpleado(int legajo) throws Exception {	
		EmpleadoBean empleado = null;
		
		String sql = "SELECT * FROM empleados WHERE legajo = " + legajo ;
		
		try{
		    Statement select = conexion.createStatement();
	        ResultSet rs = select.executeQuery(sql);
		    if (rs.next()) {
	            logger.info("Se recupero el empleado que corresponde al legajo {}.", legajo);
	            empleado = new EmpleadoBeanImpl();
	            empleado.setLegajo(rs.getInt("legajo"));
	            empleado.setApellido(rs.getString("apellido"));
	            empleado.setNombre(rs.getString("nombre"));
	            empleado.setTipoDocumento(rs.getString("doc_tipo"));
	            empleado.setNroDocumento(rs.getInt("doc_nro"));
	            empleado.setDireccion(rs.getString("direccion"));
	            empleado.setTelefono(rs.getString("telefono"));
	            empleado.setCargo("Empleado");
	            empleado.setPassword(rs.getString("password"));
	            empleado.setNroSucursal((int) Math.random());
	        } 
		    rs.close();
	        select.close();
        }
        catch (SQLException ex) {
            logger.error("SQLException: " + ex.getMessage());
            logger.error("SQLState: " + ex.getSQLState());
            logger.error("VendorError: " + ex.getErrorCode());
            throw new Exception("Error en la conexión con la BD.");
        }	

		return empleado;
		// Fin datos estáticos de prueba.
	}

}
