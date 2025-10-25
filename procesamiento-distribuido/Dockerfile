# Usamos la imagen oficial de Postgres como base
FROM postgres:latest

# Variables de entorno opcionales para postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=mysecretpassword
ENV POSTGRES_DB=clemente

# Instalar OpenJDK 24
RUN apt-get update && \
    apt-get install -y wget unzip && \
    wget https://download.java.net/java/24/latest/jdk-24_linux-x64_bin.tar.gz -O /tmp/jdk24.tar.gz && \
    mkdir -p /opt/jdk24 && \
    tar -xzf /tmp/jdk24.tar.gz -C /opt/jdk24 --strip-components=1 && \
    rm /tmp/jdk24.tar.gz

# Configurar variables de entorno de Java
ENV JAVA_HOME=/opt/jdk24
ENV PATH=$JAVA_HOME/bin:$PATH

# Crear carpeta para los archivos
RUN mkdir -p /app

# Copiar archivos JAR y CSV al contenedor
COPY app/build/libs/app.jar /app/
COPY ticketsh/build/libs/ticketsh.jar /app/
COPY ticketsd/build/libs/ticketsd.jar /app/
COPY inventario/build/libs/inventario.jar /app/
COPY TicketH.csv /app/
COPY TicketD.csv /app/

# Exponer puerto de Postgres
EXPOSE 5432

# Comando por defecto (lanzar Postgres)
CMD ["postgres"]

