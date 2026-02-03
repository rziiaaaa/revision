def list_books(mysql):
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT b.id AS id_book, b.title, a.name AS author, p.name AS publisher,
               CAST(b.price AS DOUBLE) AS price, b.year
        FROM book b
        JOIN author a ON a.id = b.author_id
        JOIN publisher p ON p.id = b.publisher_id
        ORDER BY b.title
        LIMIT 200
    """)
    rows = cur.fetchall()
    cur.close()
    return rows

def ca_by_publisher(mysql):
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT p.name AS publisher,
               CAST(SUM(s.quantity * b.price) AS DOUBLE) AS revenue
        FROM sale s
        JOIN book b ON b.id = s.book_id
        JOIN publisher p ON p.id = b.publisher_id
        GROUP BY p.name
        ORDER BY revenue DESC
    """)
    rows = cur.fetchall()
    cur.close()
    return rows

def variation_2015_2016(mysql):
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT b.title AS book,
               CAST(COALESCE(v16.qte,0) - COALESCE(v15.qte,0) AS SIGNED) AS variation_qty
        FROM book b
        LEFT JOIN (
          SELECT book_id, CAST(SUM(quantity) AS SIGNED) AS qte
          FROM sale WHERE year = 2015 GROUP BY book_id
        ) v15 ON v15.book_id = b.id
        LEFT JOIN (
          SELECT book_id, CAST(SUM(quantity) AS SIGNED) AS qte
          FROM sale WHERE year = 2016 GROUP BY book_id
        ) v16 ON v16.book_id = b.id
        ORDER BY variation_qty DESC, b.title
    """)
    rows = cur.fetchall()
    cur.close()
    return rows
