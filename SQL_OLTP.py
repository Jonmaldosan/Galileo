DDL_QUERY =  '''
CREATE TABLE ENVIO(	
    Envio_id	INT PRIMARY KEY,
    Forma_de_envio	VARCHAR(250)
);

CREATE TABLE REGION(	
    Region_id	VARCHAR(75) PRIMARY KEY,
    Region	VARCHAR(250)
);
	
CREATE TABLE DEPARTAMENTO(	
    Departamento_id	VARCHAR(250) PRIMARY KEY,
    Region_id	VARCHAR(250),
    Departamento	VARCHAR(250),
    CONSTRAINT fk_Region	
        FOREIGN KEY (Region_id)	
            REFERENCES REGION(Region_id)
);	
	
CREATE TABLE MUNICIPIO(	
    Municipio_id	VARCHAR(250) PRIMARY KEY,
    Departamento_id	VARCHAR(250),
    Municipio	VARCHAR(250),
    CONSTRAINT fk_Departamento	
        FOREIGN KEY (Departamento_id)	
            REFERENCES DEPARTAMENTO(Departamento_id)	
);	
	
CREATE TABLE CATEGORIA(	
    Categoria_id	VARCHAR(250) PRIMARY KEY,
    Categoria	VARCHAR(250)
);
	
CREATE TABLE SUBCATEGORIA(	
    Subcategoria_id	VARCHAR(250) PRIMARY KEY,
    Categoria_id	VARCHAR(250),
    Categoria	VARCHAR(250),
    Subcategoria	VARCHAR(250),
    CONSTRAINT fk_Categoria	
        FOREIGN KEY (Categoria_id)	
            REFERENCES CATEGORIA(Categoria_id)	
);
	
CREATE TABLE PRODUCTO(	
    Producto_id	VARCHAR(250) PRIMARY KEY,
    Categoria_id	VARCHAR(100),
    Subcategoria_id	VARCHAR(100),
    Nombre_del_producto	VARCHAR(250),
    CONSTRAINT fk_Categoria	
        FOREIGN KEY (Categoria_id)	
            REFERENCES CATEGORIA(Categoria_id),	
    CONSTRAINT fk_Subcategoria	
        FOREIGN KEY (Subcategoria_id)	
            REFERENCES SUBCATEGORIA(Subcategoria_id)	
);

CREATE TABLE SEGMENTO(	
    Segmento_id	VARCHAR(75) PRIMARY KEY,
    Segmento	VARCHAR(250)
);
	
CREATE TABLE CLIENTE(	
    Cliente_id	VARCHAR(75) PRIMARY KEY,
    Nombre_del_cliente	VARCHAR(250),
    Segmento_id	VARCHAR(250),
    Region_id	VARCHAR(250),
    Departamento_id	VARCHAR(250),
    Municipio_id	VARCHAR(250),	
    CONSTRAINT fk_Municipio	
        FOREIGN KEY (Municipio_id)	
            REFERENCES MUNICIPIO(Municipio_id),	
    CONSTRAINT fk_Segmento	
        FOREIGN KEY (Segmento_id)	
            REFERENCES SEGMENTO(Segmento_id)	
);

CREATE TABLE GERENTE(	
    Gerente_id	INT PRIMARY KEY,
    Gerente_regional	VARCHAR(250)
);

CREATE TABLE STORE(	
    Store_id	VARCHAR(75) PRIMARY KEY,
    Region_id	VARCHAR(75),
    Gerente_id	INT,
    Store	VARCHAR(100),
    CONSTRAINT fk_Gerente	
        FOREIGN KEY (Gerente_id)	
            REFERENCES GERENTE(Gerente_id),
    CONSTRAINT fk_Region	
        FOREIGN KEY (Region_id)	
            REFERENCES REGION(Region_id)
);

CREATE TABLE VENTAS(	
    Pedido_id	VARCHAR(250) PRIMARY KEY,
    Fecha_Pedido	DATE,
    Fecha_de_envio	DATE,
    Envio_id INT,
    Cliente_id	VARCHAR(75),
    Store_id	VARCHAR(75),
    Producto_id	VARCHAR(250),
    Total	FLOAT,
    Cantidad	INT,
    Descuento	FLOAT,
    Ganancia	FLOAT,
    CONSTRAINT fk_Envio	
        FOREIGN KEY (Envio_id)	
            REFERENCES ENVIO(Envio_id),	
    CONSTRAINT fk_Cliente	
        FOREIGN KEY (Cliente_id)	
            REFERENCES CLIENTE(Cliente_id),	
    CONSTRAINT fk_Store	
        FOREIGN KEY (Store_id)	
            REFERENCES STORE(Store_id),	
    CONSTRAINT fk_Producto	
        FOREIGN KEY (Producto_id)	
            REFERENCES PRODUCTO(Producto_id)
);

CREATE TABLE DEVOLUCION(	
    Pedido_id	VARCHAR(250) PRIMARY KEY,
    Devuelto	VARCHAR(75),
    CONSTRAINT fk_devolucion	
        FOREIGN KEY (pedido_id)	
            REFERENCES VENTAS(pedido_id)
)
'''