create table TICKETSH (
  Folio      varchar(8) primary key,
  Fecha      date       not null,
  IdEstado   int        not null,
  IdCiudad   int        not null,
  IdTienda   int        not null,
  IdEmpleado int        not null
);

create table TICKETSD (
  Ticket     varchar(8) not null,
  IdProducto int        not null,
  Unidades   int        not null,
  Precio     int        not null,
  primary key (Ticket, IdProducto)
);

