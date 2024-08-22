import psycopg2
from psycopg2.sql import SQL, Identifier


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute('''
                    DROP TABLE IF EXISTS phones;
                    DROP TABLE IF EXISTS client;
                    ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS client(
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(60) NOT NULL,
                email VARCHAR(60) UNIQUE
        );
        ''')

        cur.execute('''
                    CREATE TABLE IF NOT exists phones(
                            phone_id SERIAL PRIMARY KEY,
                            client_id INT NOT NULL REFERENCES client(client_id),
                            phone_number BIGINT NOT NULL
                    );
                ''')
        return conn.commit()


def add_client(conn, first_name, last_name, email):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO client(first_name, last_name, email) VALUES (%s, %s, %s);
            ''', (first_name, last_name, email))
        return conn.commit()


def add_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO phones(client_id, phone_number) VALUES (%s, %s);
            ''', (client_id, phone_number))
        return conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        kwargs = {'first_name': first_name, 'last_name': last_name, 'email': email, 'phone_number': phone_number}
        for key, value in kwargs.items():
            if value:
                cur.execute(SQL('UPDATE client SET {}=%s WHERE client_id=%s').format(Identifier(key)), (value, client_id))
        cur.execute('''
                SELECT * FROM client
                WHERE client_id = %s
                ''', (client_id,))
        return cur.fetchall()


def delete_phone(conn, phone_number):
    with conn.cursor() as cur:
        cur.execute('''
            DELETE FROM phones WHERE phone_number=%s;
            ''', (phone_number,))
        return conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute('''
            DELETE FROM phones WHERE client_id=%s;
            ''', (client_id,))
        cur.execute('''
            DELETE FROM client WHERE client_id=%s;
            ''', (client_id,))
        conn.commit()


def search_client(conn, first_name=None, last_name=None, email=None, phone_number=None):
    with conn.cursor():
        kwargs = {'first_name': first_name, 'last_name': last_name, 'email': email, 'phone_number': phone_number}
        return '''
                SELECT * FROM client c
                    LEFT JOIN phones p ON c.client_id = p.client_id
                    WHERE
                        (first_name=%(first_name)s OR %(first_name)s IS NULL) AND
                        (last_name=%(last_name)s OR %(last_name)s IS NULL) AND
                        (email=%(email)s OR %(email)s IS NULL) AND
                        (phone_number=%(phone_number)s OR %(phone_number)s IS NULL);
                    ''', (kwargs,)


with psycopg2.connect(database='clients', user='postgres', password='z&Collab7') as connect:
    create_tables(connect)

    add_client(connect, 'Lena', 'Zaharova', '12435@gmaoil.com')
    add_client(connect, 'Artem', 'Ivanov', 'artem@mail.ru')
    add_client(connect, 'Maria', 'Markova', 'www@gmail.com')

    add_phone(connect, 1, 89123456789)
    add_phone(connect, 1, 89009002222)
    add_phone(connect, 2, 89009002121)

    print(change_client(connect, 1, first_name='Dasha', email='snow@gmail.com'))

    delete_phone(connect, 89123456789)

    delete_client(connect, 2)

    query_2, params_2 = search_client(connect, first_name='Dasha', email='12435@gmal.com')
    print(query_2)
    print(params_2)
