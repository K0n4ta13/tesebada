CREATE TABLE authors (
    author_id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    nationality VARCHAR(50)
);

CREATE TABLE books (
    isbn VARCHAR(20) PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    original_title VARCHAR(200),
    genre VARCHAR(50),
    language_code VARCHAR(10) NOT NULL,
    release_year INT NOT NULL,
    synopsis TEXT,
    publisher_name VARCHAR(100) NOT NULL,
    publisher_country VARCHAR(50),
    edition_type VARCHAR(10) CHECK (edition_type IN ('print', 'digital')),
    print_cover VARCHAR(20) CHECK (print_cover IN ('hardcover', 'paperback')),
    print_pages INT,
    digital_format VARCHAR(10) CHECK (digital_format IN ('pdf', 'epub')),
    author_id VARCHAR(10),
    CONSTRAINT fk_author FOREIGN KEY (author_id) REFERENCES authors(author_id)
);


INSERT INTO Authors (author_id, name, nationality) VALUES
('JR', 'Juan Rulfo', 'Mexicana'),
('JJA', 'Juan José Arreola', 'Mexicana'),
('MVL', 'Mario Vargas Llosa', 'Peruana'),
('OP', 'Octavio Paz', 'Mexicana'),
('YK', 'Yasunari Kawabata', 'Japonesa'),
('KO', 'Kenzaburō Ōe', 'Japonesa'),
('YM', 'Yukio Mishima', 'Japonesa'),
('OD', 'Osamu Dazai', 'Japonesa'),
('DA', 'Dante Alighieri', 'Italiana'),
('FD', 'Fiódor Dostoyevski', 'Rusa'),
('FK', 'Franz Kafka', 'Checa'),
('GO', 'George Orwell', 'Británica'),
('HM', 'Haruki Murakami', 'Japonesa');

INSERT INTO Books (isbn, title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages, synopsis) 
VALUES ('978-84-376-0418-3', 'Pedro Páramo', 'Novela', 'es', 1955, 'JR', 'Fondo de Cultura Económica', 'México', 'print', 'paperback', 132, 'Una de las obras maestras de la literatura hispanoamericana.');
INSERT INTO Books (isbn, title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, digital_format) 
VALUES ('978-607-16-1662-7', 'Confabulario', 'Cuento', 'es', 1952, 'JJA', 'Joaquín Mortiz', 'México', 'digital', 'epub');
INSERT INTO Books (isbn, title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-204-7183-9', 'La ciudad y los perros', 'Novela', 'es', 1963, 'MVL', 'Seix Barral', 'España', 'print', 'hardcover', 448);
INSERT INTO Books (isbn, title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, digital_format) 
VALUES ('978-84-376-0239-4', 'El laberinto de la soledad', 'Ensayo', 'es', 1950, 'OP', 'Cuadernos Americanos', 'México', 'digital', 'pdf');
INSERT INTO Books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-95501-14-1', 'Lo bello y lo triste', 'Utsukushisa to Kanashimi to', 'Novela', 'jp', 1964, 'YK', 'Emecé', 'Argentina', 'print', 'paperback', 224);
INSERT INTO Books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-339-1457-6', 'Una cuestión personal', 'Kojinteki na taiken', 'Novela', 'jp', 1964, 'KO', 'Anagrama', 'España', 'print', 'paperback', 192);
INSERT INTO Books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-206-3333-6', 'Confesiones de una máscara', 'Kamen no Kokuhaku', 'Novela', 'jp', 1949, 'YM', 'Alianza Editorial', 'España', 'print', 'paperback', 256);
INSERT INTO Books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, digital_format) 
VALUES ('978-84-937407-1-9', 'Indigno de ser humano', 'Ningen Shikkaku', 'Novela', 'jp', 1948, 'OD', 'Sajalín Editores', 'España', 'digital', 'epub');
INSERT INTO Books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-376-0336-0', 'La divina comedia', 'Divina Commedia', 'Poesía Épica', 'it', 1472, 'DA', 'Cátedra', 'España', 'print', 'hardcover', 860);
INSERT INTO Books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-206-5381-5', 'Los hermanos Karamázov', 'Brat''ya Karamazovy', 'Novela filosófica', 'ru', 1880, 'FD', 'Alianza Editorial', 'España', 'print', 'paperback', 1120);
INSERT INTO Books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, digital_format) 
VALUES ('978-84-206-3554-5', 'El proceso', 'Der Process', 'Novela', 'de', 1925, 'FK', 'Die Schmiede', 'Alemania', 'digital', 'pdf');
INSERT INTO Books (isbn, title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-0-141-03614-4', '1984', 'Ciencia Ficción', 'en', 1949, 'GO', 'Secker and Warburg', 'Reino Unido', 'print', 'paperback', 328);
INSERT INTO Books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-8383-561-2', 'Hombres sin mujeres', 'Onna no inai otokotachi', 'Cuento', 'jp', 2014, 'HM', 'Tusquets', 'España', 'print', 'paperback', 288);
