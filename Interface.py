import psycopg2
import io

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratings_table_name, file_path, open_connection):
    cursor = open_connection.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {ratings_table_name} (
        userid INT,
        movieid INT,
        rating FLOAT
    );
    """
    cursor.execute(create_table_query)  # tạo bảng ratings nếu chưa tồn tại
    open_connection.commit()

    formatted_data = io.StringIO()
    with open(file_path, 'r') as f:
        for line in f:
            # data từng dòng:  1::122::5::838985046
            parts = line.strip().split('::')  # Tách theo '::'
            if len(parts) == 4:  # kiểm tra nếu đủ 4 part
                formatted_data.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")  # ghi 3 phần đầu, cách nhau bởi dấu tab
            else:
                print(f"Dòng sai định dạng (userid, movieid, rating, timestamp): {line.strip()}")

    # đưa con trỏ StringIO về đầu trước khi chèn vào bảng ratings
    formatted_data.seek(0)

    cursor.copy_from(formatted_data, ratings_table_name, sep='\t')  # thêm vào bảng, phân cách bởi dấu tab
    open_connection.commit()


def rangepartition(ratings_table_name, N, open_connection):
    cursor = open_connection.cursor()
    temp_table_name = "temp_range_ratings_for_partition"

    # xóa bảng range partition nếu tồn tại
    for i in range(N):
        drop_table_query = f"DROP TABLE IF EXISTS range_part{i} CASCADE;"
        cursor.execute(drop_table_query)
    open_connection.commit()

    create_partition_table_query = f"""
    CREATE TABLE range_part{{}} (
        userid INT,
        movieid INT,
        rating FLOAT
    );
    """
    for i in range(N):
        cursor.execute(create_partition_table_query.format(i))
    open_connection.commit()

    min_rating = 0.0
    max_rating = 5.0
    step = max_rating / N

    # tạo các câu WHEN để tạo bảng trung gian
    case_clauses = []
    for i in range(N):
        lower_bound = min_rating + i * step
        upper_bound = min_rating + (i + 1) * step
        if i == 0:
            case_clauses.append(
                f"WHEN rating >= {lower_bound} AND rating <= {upper_bound} THEN {i}")
        else:
            case_clauses.append(
                f"WHEN rating > {lower_bound} AND rating <= {upper_bound} THEN {i}")

    # tạo bảng trung gian, gồm 4 cột: userid, movieid, rating, range_idx (cột cuối để xác định thuộc phân mảnh nào)
    # {' '.join(case_clauses)} -> nối với nhau bằng dấu cách, để tạo câu lệnh CASE WHEN
    create_temp_table_query = f"""
    CREATE TEMPORARY TABLE {temp_table_name} AS
    SELECT
        userid,
        movieid,
        rating,
        CASE
            {' '.join(case_clauses)}
        END AS range_idx
    FROM {ratings_table_name};
    """
    cursor.execute(create_temp_table_query)
    open_connection.commit()

    # chuyển data từ bảng trung gian qua các phân mảnh
    for i in range(N):
        insert_query = f"""
        INSERT INTO range_part{i} (userid, movieid, rating)
        SELECT userid, movieid, rating
        FROM {temp_table_name}
        WHERE range_idx = {i};
        """
        cursor.execute(insert_query)
    open_connection.commit()
    cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name} CASCADE;") # xóa bảng trung gian
    open_connection.commit()


def roundrobinpartition(ratings_table_name, N, open_connection):
    cursor = open_connection.cursor()
    temp_table_name = "temp_numbered_ratings_for_partition"

    # xóa bảng round robin partition nếu tồn tại
    for i in range(N):
        drop_table_query = f"DROP TABLE IF EXISTS rrobin_part{i} CASCADE;"
        cursor.execute(drop_table_query)
    open_connection.commit()

    create_partition_table_query = f"""
    CREATE TABLE rrobin_part{{}} (
        userid INT,
        movieid INT,
        rating FLOAT
    );
    """
    for i in range(N):
        cursor.execute(create_partition_table_query.format(i))
    open_connection.commit()

    create_temp_table_query = f"""
    CREATE TEMPORARY TABLE {temp_table_name} AS
    SELECT
        userid,
        movieid,
        rating,
        ROW_NUMBER() OVER (ORDER BY ctid) AS row_number
    FROM {ratings_table_name};
    """
    cursor.execute(create_temp_table_query)
    open_connection.commit()

    # truyền data từ bảng trung gian sang các phân mảnh
    for i in range(N):
        insert_query = f"""
        INSERT INTO rrobin_part{i} (userid, movieid, rating)
        SELECT userid, movieid, rating
        FROM {temp_table_name}
        WHERE MOD((row_number - 1), {N}) = {i};
        """
        # row_number phải trừ 1 là vì hàm ROW_NUMBER() tính từ 1, nhưng các phân mảnh tính từ 0
        cursor.execute(insert_query)
    open_connection.commit()
    cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name} CASCADE;")
    open_connection.commit()


def roundrobininsert(ratings_table_name, UserID, ItemID, Rating, open_connection):
    cursor = open_connection.cursor()

    # thêm vào bảng ratings chung
    insert_main_query = f"""
    INSERT INTO {ratings_table_name} (userid, movieid, rating)
    VALUES (%s, %s, %s);
    """
    cursor.execute(insert_main_query, (UserID, ItemID, Rating))
    open_connection.commit()

    num_rrobin_partitions = count_partitions('rrobin_part', open_connection)

    if num_rrobin_partitions == 0:
        print("Không tồn tại bảng phân mảnh round robin nào")
        return

    get_total_rows_query = f"SELECT COUNT(*) FROM {ratings_table_name};"
    cursor.execute(get_total_rows_query)
    total_rows_after_insert = cursor.fetchone()[0]

    # phải trừ 1 là vì hàm ROW_NUMBER() tính từ 1, nhưng các phân mảnh tính từ 0
    partition_index = (total_rows_after_insert - 1) % num_rrobin_partitions
    target_rrobin_table = f"rrobin_part{partition_index}"

    # thêm vào phân mảnh
    insert_rrobin_query = f"""
    INSERT INTO {target_rrobin_table} (userid, movieid, rating)
    VALUES (%s, %s, %s);
    """
    cursor.execute(insert_rrobin_query, (UserID, ItemID, Rating))
    open_connection.commit()


def rangeinsert(ratings_table_name, UserID, ItemID, Rating, open_connection):
    cursor = open_connection.cursor()

    # thêm vào bảng ratings chung
    insert_main_query = f"""
    INSERT INTO {ratings_table_name} (userid, movieid, rating)
    VALUES (%s, %s, %s);
    """
    cursor.execute(insert_main_query, (UserID, ItemID, Rating))
    open_connection.commit()

    num_range_partitions = count_partitions('range_part', open_connection)

    if num_range_partitions == 0:
        print("Không tồn tại phân mảnh range nào")
        return

    min_rating = 0.0
    max_rating = 5.0
    step = max_rating / num_range_partitions

    target_partition_index = -1
    for i in range(num_range_partitions):
        lower_bound = min_rating + i * step
        upper_bound = min_rating + (i + 1) * step

        if i == 0:
            if lower_bound <= Rating <= upper_bound:
                target_partition_index = i
                break
        else:
            if lower_bound < Rating <= upper_bound:
                target_partition_index = i
                break

    # kiểm tra nếu Rating = 5.0 nhưng không thỏa mãn đk if else ở vòng for trên -> do floating point
    if target_partition_index == -1 and Rating == max_rating:
        target_partition_index = num_range_partitions - 1

    target_range_table = f"range_part{target_partition_index}"

    # thêm vào phân mảnh
    insert_range_query = f"""
    INSERT INTO {target_range_table} (userid, movieid, rating)
    VALUES (%s, %s, %s);
    """
    cursor.execute(insert_range_query, (UserID, ItemID, Rating))
    open_connection.commit()



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
