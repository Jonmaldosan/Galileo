DDL_QUERY =  '''
CREATE TABLE cliente(
    cliente_id VARCHAR(250) PRIMARY KEY,
    nombre_del_cliente VARCHAR(250),
    municipio VARCHAR(250),
    departamento VARCHAR(250),
    region	VARCHAR(250),
    segmento VARCHAR(250)
);

CREATE TABLE producto(
    producto_id	VARCHAR(250) PRIMARY KEY,
    nombre_del_producto	VARCHAR(250),
    categoria VARCHAR(250),
    subcategoria VARCHAR(250)
);

CREATE TABLE store(
    store_id VARCHAR(250) PRIMARY KEY,
    store VARCHAR(250),
    region VARCHAR(250),
    gerente_regional VARCHAR(250)
);

CREATE TABLE municipio(
    municipio_id VARCHAR(250) PRIMARY KEY,
    municipio VARCHAR(250),
    departamento VARCHAR(250),
    region VARCHAR(250)
);

CREATE TABLE fecha(
    id_fecha VARCHAR(250) PRIMARY KEY,
    fecha_pedido DATE, 
    year INT,
    month INT,
    quarter INT,
    day INT,
    week INT,
    dayofweek INT,	
    is_weekend INT
);

CREATE TABLE fact_table(
    pedido_id VARCHAR(250) PRIMARY KEY,
    fecha_pedido DATE,
    envio_id INT,
    cliente_id VARCHAR(250),
    municipio_id VARCHAR(75),
    departamento_id VARCHAR(75),
    region_id	VARCHAR(75),
    store_id VARCHAR(75),
    producto_id	VARCHAR(250),
    total FLOAT,
    cantidad INT,
    descuento FLOAT,
    ganancia FLOAT,
    id_fecha VARCHAR(250),
    CONSTRAINT fk_cliente
            FOREIGN KEY (cliente_id)	
                REFERENCES cliente(cliente_id),
    CONSTRAINT fk_municipio
            FOREIGN KEY (municipio_id)	
                REFERENCES municipio(municipio_id),
    CONSTRAINT fk_producto
            FOREIGN KEY (producto_id)	
                REFERENCES producto(producto_id),
    CONSTRAINT fk_store
            FOREIGN KEY (store_id)	
                REFERENCES store(store_id),
    CONSTRAINT fk_fecha
            FOREIGN KEY (id_fecha)	
                REFERENCES fecha(id_fecha)
)
'''