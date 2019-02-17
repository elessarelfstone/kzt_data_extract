import settings
import os
import psycopg2


class MetaDb:
    @staticmethod
    def get_listdict_from_cur(cur):
        col_names = [desc[0] for desc in cur.description]
        rows = []
        for row in cur.fetchall():
            rw = {}
            for name, value in zip(col_names, row):
                rw[name] = value
            rows.append(rw)
        return rows

    @staticmethod
    def make_conn():
        server = os.getenv("META_SERVER_HOST")
        db=os.getenv("META_SERVER_DB")
        user = os.getenv("META_SERVER_USER")
        password = os.getenv("META_SERVER_PASS")
        return psycopg2.connect("host={} dbname={} user={} password={}".format(server, db, user, password))

    @staticmethod
    def get_data_sources(source_type_code):
        conn = MetaDb.make_conn()
        sql = """
                select
                     ds.code,
                     ds.params::jsonb || dst.params::jsonb || ds.conn_detail::jsonb as params,
                     ds.conn_detail
                from meta.data_source_type dst, meta.data_source ds
                where dst.data_source_type_id = ds.data_source_type_id and dst.code = %s and ds.is_active = True
        """
        cur = conn.cursor()
        cur.execute(sql, (source_type_code,))
        return MetaDb.get_listdict_from_cur(cur)

    @staticmethod
    def get_data_source_tables(source_type_code):
        conn = MetaDb.make_conn()
        sql = """
                select
                     dstb.name                     
                from meta.data_source_type dst,
                     meta.data_source_table dstb
                where dst.data_source_type_id = dstb.data_source_type_id  and
                      dstb.is_active = True and
                      dst.code = %s
                """
        cur = conn.cursor()
        cur.execute(sql, (source_type_code,))
        return MetaDb.get_listdict_from_cur(cur)



    @staticmethod
    def get_script_template(src_code, table):
        conn = MetaDb.make_conn()
        sql = """
                    select meta.get_sql_template(%s, %s)
                """
        cur = conn.cursor()
        cur.execute(sql, (table, src_code,))
        templ = cur.fetchone()
        return templ


# script_template = MetaDb.get_script_template("asr", "db.tdr")
# print(script_template)

