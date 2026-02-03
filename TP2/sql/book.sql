CREATE DATABASE IF NOT EXISTS books CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE books;

-- Auteurs / Éditeurs
CREATE TABLE author (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(120) NOT NULL
);

CREATE TABLE publisher (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(120) NOT NULL
);

-- Livres
CREATE TABLE book (
  id INT AUTO_INCREMENT PRIMARY KEY,
  title VARCHAR(200) NOT NULL,
  author_id INT NOT NULL,
  publisher_id INT NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  year INT NOT NULL,
  FOREIGN KEY (author_id) REFERENCES author(id),
  FOREIGN KEY (publisher_id) REFERENCES publisher(id)
);

-- Ventes (agrégées au jour, pour faire simple)
CREATE TABLE sale (
  id INT AUTO_INCREMENT PRIMARY KEY,
  book_id INT NOT NULL,
  year INT NOT NULL,        -- 2015 ou 2016 pour la démo
  quantity INT NOT NULL,
  FOREIGN KEY (book_id) REFERENCES book(id)
);

-- Jeux de données mini
INSERT INTO author (name) VALUES
  ('George Orwell'), ('J.K. Rowling'), ('Haruki Murakami');

INSERT INTO publisher (name) VALUES
  ('Penguin'), ('Bloomsbury'), ('Knopf');

INSERT INTO book (title, author_id, publisher_id, price, year) VALUES
  ('1984', 1, 1, 12.90, 1949),
  ('Animal Farm', 1, 1, 9.90, 1945),
  ('Harry Potter and the Philosopher''s Stone', 2, 2, 19.90, 1997),
  ('Kafka on the Shore', 3, 3, 14.90, 2002);

INSERT INTO sale (book_id, year, quantity) VALUES
  (1, 2015, 1200), (1, 2016, 1500),
  (2, 2015, 900),  (2, 2016, 800),
  (3, 2015, 3000), (3, 2016, 3500),
  (4, 2015, 700),  (4, 2016, 600);
