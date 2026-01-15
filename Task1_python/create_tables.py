from db_connection import get_connection


def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            birthday DATE,
            sex CHAR(1),
            room INTEGER,
            FOREIGN KEY (room) REFERENCES rooms(id)
        );
    """)
    conn.commit()
