import psycopg2

def drop_tables(conn):
    with (conn.cursor() as cur):
        cur.execute("""DROP TABLE client, phone;""")

def create_tables(conn):
    with (conn.cursor() as cur):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client(
            id SERIAL PRIMARY KEY,
            firstname VARCHAR (60) NOT NULL,
            lastname VARCHAR (60) NOT NULL,
            email VARCHAR (40) NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone(
            id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES client(id),
            number VARCHAR (30) UNIQUE NOT NULL
        );
        """)
    conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    with (conn.cursor() as cur):
        cur.execute("INSERT INTO client (firstname, lastname, email) VALUES (%s,%s,%s) RETURNING id;",
                    (first_name, last_name, email))

        client_id = cur.fetchone()[0]
        conn.commit()

    if phones:
        add_phones(conn, client_id, phones)


def add_phone(conn, client_id, phone):
    with (conn.cursor() as cur):
        cur.execute("INSERT INTO phone(client_id, number) VALUES (%s,%s);",
                    (client_id, phone))
        conn.commit()

def delete_phones(conn, client_id):
    with (conn.cursor() as cur):
        cur.execute("DELETE FROM phone WHERE client_id = %s;",
                    (client_id,))
        conn.commit()

def add_phones(conn, client_id, phones):
    with (conn.cursor() as cur):
        phone_values = ((client_id, phone) for phone in phones)
        cur.executemany("INSERT INTO phone(client_id, number) VALUES (%s,%s); ", phone_values)
        conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    # если в функцию change_client передано хотя бы
    # одна переменная из перечисленных внизу, то
    # осуществляем запрос UPDATE к БД
    if first_name or last_name or email:
        with (conn.cursor() as cur):
            cur.execute("UPDATE client SET firstname = COALESCE(%s, firstname), lastname = COALESCE(%s, lastname), email = COALESCE(%s, email) WHERE id = %s;" ,
                        (first_name, last_name, email, client_id))
            conn.commit()

    if phones:
        # удаляем старые телефонЫ клиента
        delete_phones(conn, client_id)
        # в функцию change_client был передан аргумент phones
        # это список телефонов, кот. должен быть у клиента
        # удалив все его старые тел на предыдущем шаге
        # мы можем использовать add_phones для добавления новых
        add_phones(conn, client_id, phones)

def delete_phone(conn, client_id, phone):
    with (conn.cursor() as cur):
        cur.execute("DELETE FROM phone WHERE client_id=%s AND number=%s;",
                    (client_id, phone))
        conn.commit()

def delete_client(conn, client_id):
    with (conn.cursor() as cur):
        cur.execute("DELETE FROM client WHERE id=%s;",
                    (client_id,))
        conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with (conn.cursor() as cur):
        cur.execute("""SELECT * FROM client c 
                    JOIN phone p on c.id = p.client_id 
                    WHERE c.firstname = COALESCE(%s, c.firstname) 
                    AND c.lastname = COALESCE(%s, c.lastname) 
                    AND c.email = COALESCE(%s, c.email) 
                    AND p.number = COALESCE(%s, p.number);""" ,
                     (first_name, last_name, email, phone))

        print(cur.fetchall())

with psycopg2.connect(database="client_db", user="postgres", password="") as conn:
    drop_tables(conn)
    create_tables(conn)
    add_client(conn, 'Иван', 'Петров', 'petrov@mail.ru', ['79096785678', '79035678767'])
    add_client(conn, 'Сергей', 'Иванов', 'ivanov@mail.ru', ['79096785670', '79035678798'])
    add_client(conn, 'Алексей', 'Сидоров', 'sidorov@mail.ru')
    change_client(conn, 2, first_name ='Петр', last_name = 'Соколов')
    add_phone(conn, 1, phone = '79267869050')
    delete_phone(conn, 1, phone = '79035678767')
    delete_client(conn, 3)
    find_client(conn, first_name='Иван')
conn.close()



