# Creo de la Base de Datos.
CREATE DATABASE vuelos;
# Selecciono la base de datos sobre las que se haran las modificaciones.
USE vuelos;
# ---------------------------------------------------------------------------------------------------------------------------------------------------

# Creacion de Tablas en la Base de Datos.

CREATE TABLE ubicaciones (
    pais VARCHAR(20) NOT NULL,
    estado VARCHAR(20) NOT NULL,
    ciudad VARCHAR(20) NOT NULL,
    huso INT NOT NULL CHECK (huso >= -12 AND huso <= 12),

    CONSTRAINT pk_ubicaciones
    PRIMARY KEY (pais, estado, ciudad)
) ENGINE = InnoDB;

CREATE TABLE aeropuertos (
    codigo VARCHAR(45) NOT NULL,
    nombre VARCHAR(40) NOT NULL,
    telefono VARCHAR(15) NOT NULL,
    direccion VARCHAR(30) NOT NULL,
    pais VARCHAR(20) NOT NULL,
    estado VARCHAR(20) NOT NULL,
    ciudad VARCHAR(20) NOT NULL,

    CONSTRAINT pk_aeropuertos
    PRIMARY KEY (codigo),

    CONSTRAINT FK_aeropuertos_pais
    FOREIGN KEY (pais, estado, ciudad) REFERENCES ubicaciones(pais, estado, ciudad)
        ON DELETE RESTRICT ON UPDATE CASCADE

) ENGINE = InnoDB;

CREATE TABLE vuelos_programados (
    numero VARCHAR(10) NOT NULL,
    aeropuerto_salida VARCHAR(45) NOT NULL,
    aeropuerto_llegada VARCHAR(45) NOT NULL,

    CONSTRAINT pk_vuelos_programados
    PRIMARY KEY (numero),

    CONSTRAINT FK_vuelos_programados_aeropuerto_llegada
    FOREIGN KEY (aeropuerto_llegada) REFERENCES aeropuertos(codigo)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT FK_vuelos_programados_aeropuerto_salida
    FOREIGN KEY (aeropuerto_salida) REFERENCES aeropuertos(codigo)
        ON DELETE RESTRICT ON UPDATE CASCADE
    
) ENGINE = InnoDB;

CREATE TABLE modelos_avion (
    modelo VARCHAR(20) NOT NULL,
    fabricante VARCHAR(20) NOT NULL,
    cabinas INT UNSIGNED NOT NULL,
    cant_asientos INT UNSIGNED NOT NULL,
    
    CONSTRAINT pk_modelo
    PRIMARY KEY (modelo)
) ENGINE = InnoDB;

CREATE TABLE salidas (
    vuelo VARCHAR(10) NOT NULL,
    dia ENUM ('Do', 'Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa'),
    hora_sale TIME NOT NULL,
    hora_llega TIME NOT NULL,
    modelo_avion VARCHAR(20) NOT NULL,
    
    CONSTRAINT pk_salidas
    PRIMARY KEY (vuelo, dia),

    CONSTRAINT FK_salidas_vuelo
    FOREIGN KEY (vuelo) REFERENCES vuelos_programados(numero) 
        ON DELETE RESTRICT ON UPDATE CASCADE,
    
    CONSTRAINT FK_salidas_modelo_avion
    FOREIGN KEY (modelo_avion) REFERENCES modelos_avion(modelo)
        ON DELETE RESTRICT ON UPDATE CASCADE

) ENGINE = InnoDB;

CREATE TABLE instancias_vuelo (
    vuelo VARCHAR(10) NOT NULL,
    fecha DATE NOT NULL,
    dia ENUM ('Do', 'Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa') NOT NULL,
    estado VARCHAR(15),

    CONSTRAINT pk_instancias_vuelo
    PRIMARY KEY (vuelo, fecha),

    CONSTRAINT FK_instancias_vuelo_vuelo 
    FOREIGN KEY (vuelo, dia) REFERENCES salidas(vuelo, dia)
        ON DELETE RESTRICT ON UPDATE CASCADE

) ENGINE = InnoDB;

CREATE TABLE clases (
    nombre VARCHAR(20) NOT NULL,
    porcentaje DECIMAL(2,2) UNSIGNED NOT NULL,

    CONSTRAINT pk_nombre
    PRIMARY KEY (nombre)
) ENGINE = InnoDB;

CREATE TABLE comodidades(
    codigo INT UNSIGNED NOT NULL,
    descripcion TEXT NOT NULL,

    CONSTRAINT pk_comodidades
    PRIMARY KEY (codigo)

) ENGINE = InnoDB;

CREATE TABLE pasajeros(
    doc_tipo VARCHAR(45) NOT NULL,
    doc_nro INT UNSIGNED NOT NULL,
    apellido VARCHAR(20) NOT NULL,
    nombre VARCHAR(20) NOT NULL,
    direccion VARCHAR(40) NOT NULL,
    telefono VARCHAR(15) NOT NULL,
    nacionalidad VARCHAR(20) NOT NULL,

    CONSTRAINT pk_pasajeros
    PRIMARY KEY(doc_tipo, doc_nro)
    
) ENGINE = InnoDB;

CREATE TABLE empleados(
    legajo INT UNSIGNED NOT NULL,
    password VARCHAR(32) NOT NULL,
    doc_tipo VARCHAR(45) NOT NULL,
    doc_nro INT UNSIGNED NOT NULL,
    apellido VARCHAR(20) NOT NULL,
    nombre VARCHAR(20) NOT NULL,
    direccion VARCHAR(40) NOT NULL,
    telefono VARCHAR(15) NOT NULL,

    CONSTRAINT pk_empleados
    PRIMARY KEY (legajo)
    
) ENGINE = InnoDB;

CREATE TABLE brinda(
    vuelo VARCHAR(10) NOT NULL,
    dia ENUM ('Do', 'Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa'),
    precio DECIMAL(7,2) UNSIGNED NOT NULL,
    clase VARCHAR(20) NOT NULL,
    cant_asientos SMALLINT UNSIGNED NOT NULL,
    
    CONSTRAINT pk_brinda
    PRIMARY KEY(vuelo, dia, clase),

    CONSTRAINT FK_brinda_vuelo
    FOREIGN KEY (vuelo, dia) REFERENCES salidas(vuelo, dia)
    ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT FK_brinda_clase
    FOREIGN KEY (clase) REFERENCES clases(nombre) 
    ON DELETE RESTRICT ON UPDATE CASCADE
    
) ENGINE = InnoDB;

CREATE TABLE posee(
    clase VARCHAR(20) NOT NULL,
    comodidad INT UNSIGNED NOT NULL,
    
    CONSTRAINT pk_posee
    PRIMARY KEY (clase, comodidad),    

    CONSTRAINT FK_posee_clase
    FOREIGN KEY (clase) REFERENCES clases(nombre) 
    ON DELETE RESTRICT ON UPDATE CASCADE,
    
    CONSTRAINT FK_posee_comodidad
    FOREIGN KEY (comodidad) REFERENCES comodidades(codigo) 
    ON DELETE RESTRICT ON UPDATE CASCADE    

) ENGINE = InnoDB;

CREATE TABLE reservas(
    numero INT UNSIGNED AUTO_INCREMENT NOT NULL,    
    fecha DATE NOT NULL,
    vencimiento DATE NOT NULL,
    estado VARCHAR(15) NOT NULL,
    doc_tipo VARCHAR(45) NOT NULL,
    doc_nro INT UNSIGNED NOT NULL,
    legajo INT UNSIGNED NOT NULL,

    CONSTRAINT pk_reservas
    PRIMARY KEY (numero),

    CONSTRAINT FK_reservas_doc_tipo
    FOREIGN KEY (doc_tipo, doc_nro) REFERENCES pasajeros(doc_tipo, doc_nro) 
    ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT FK_reservas_legajo
    FOREIGN KEY (legajo) REFERENCES empleados(legajo) 
    ON DELETE RESTRICT ON UPDATE CASCADE
    
) ENGINE=  InnoDB;

CREATE TABLE reserva_vuelo_clase(
    numero INT UNSIGNED NOT NULL,     
    vuelo VARCHAR(10) NOT NULL,
    fecha_vuelo DATE NOT NULL,
    clase VARCHAR(20) NOT NULL,

    CONSTRAINT pk_reserva_vuelo_clase
    PRIMARY KEY(numero,vuelo, fecha_vuelo),

    CONSTRAINT FK_reserva_vuelo_clase_numero
    FOREIGN KEY (numero) REFERENCES reservas(numero) 
    ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT FK_reserva_vuelo_clase_vuelo
    FOREIGN KEY (vuelo, fecha_vuelo) REFERENCES instancias_vuelo(vuelo, fecha) 
    ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT FK_reserva_vuelo_clase_clase
    FOREIGN KEY (clase) REFERENCES clases(nombre) 
    ON DELETE RESTRICT ON UPDATE CASCADE

) ENGINE = InnoDB;

CREATE TABLE asientos_reservados(
    vuelo VARCHAR(10) NOT NULL,
    fecha DATE NOT NULL,
    clase VARCHAR(20) NOT NULL,
    cantidad INT UNSIGNED NOT NULL,
    
    CONSTRAINT pk_asientos_reservados
    PRIMARY KEY(vuelo, fecha, clase),
    
    CONSTRAINT FK_asientos_reservados_vuelo
    FOREIGN KEY (vuelo, fecha) REFERENCES instancias_vuelo(vuelo, fecha) 
    ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT FK_asientos_reservados_clase
    FOREIGN KEY (clase) REFERENCES clases (nombre) 
    ON DELETE RESTRICT ON UPDATE CASCADE
    
) ENGINE = InnoDB;

# -----------------------------------------------------------------------------------------------------------------------------------------

# Creacion de usuarios y otorgamiento de privilegios

# Usuario administrador
# el usuario admin con password 'pwadmin' puede conectarse solo desde la computadora donde se encuentra el servidor de MySQL (localhost) 
# luego le otorgo privilegios utilizando la sentencia 'GRANT'
CREATE USER 'admin'@'localhost' IDENTIFIED BY 'admin';
GRANT ALL PRIVILEGES ON vuelos.* TO 'admin'@'localhost' WITH GRANT OPTION;

# Usuario empleado
CREATE USER 'empleado' IDENTIFIED BY 'empleado';
GRANT SELECT ON vuelos.* TO 'empleado';
GRANT UPDATE, DELETE, INSERT ON vuelos.reservas TO 'empleado';
GRANT UPDATE, DELETE, INSERT ON vuelos.pasajeros TO 'empleado';
GRANT UPDATE, DELETE, INSERT ON vuelos.reserva_vuelo_clase TO 'empleado';

CREATE VIEW vuelos_disponibles AS
    SELECT S.vuelo AS 'nro_vuelo',
            S.modelo_avion AS 'modelo',
            IV.fecha AS 'fecha',
            S.dia AS 'dia_sale',
            S.hora_sale AS 'hora_sale',
            S.hora_llega AS 'hora_llega',
            if(S.hora_llega >= s.hora_sale, timediff(s.hora_llega, s.hora_sale), addtime('24:00:00', timediff(s.hora_llega, s.hora_sale))) AS 'tiempo_estimado',
            VP.aeropuerto_salida AS 'codigo_aero_sale',
            VS.nombre AS 'nombre_aero_sale',
            VS.ciudad AS 'ciudad_sale',
            VS.estado AS 'estado_sale',
            VS.pais AS 'pais_sale',
            VP.aeropuerto_llegada AS 'codigo_aero_llega',
            VL.nombre AS 'nombre_aero_llega',
            VL.ciudad AS 'ciudad_llega',
            VL.estado AS 'estado_llega',
            VL.pais AS 'pais_llega',
            B.precio AS 'precio',
            ROUND(B.cant_asientos + (C.porcentaje * B.cant_asientos) - (AR.cantidad)) AS 'asientos_disponibles',
            AR.clase AS 'clase'
    FROM (salidas S
        JOIN instancias_vuelo AS IV ON (S.vuelo = IV.vuelo and S.dia = IV.dia) 
        JOIN vuelos_programados AS VP ON (S.vuelo = VP.numero)
        JOIN aeropuertos AS VS ON (VS.codigo = VP.aeropuerto_salida)
	    JOIN aeropuertos AS VL ON (VL.codigo = VP.aeropuerto_llegada)
        JOIN brinda AS B ON (B.vuelo = S.vuelo and B.dia = S.dia)
        JOIN asientos_reservados AS AR ON (AR.vuelo = IV.vuelo and AR.fecha = IV.fecha and AR.clase = B.clase)
        JOIN clases AS C ON (AR.clase = C.nombre));
        

CREATE USER 'cliente' IDENTIFIED BY 'cliente';
GRANT SELECT ON vuelos.vuelos_disponibles TO 'cliente';
