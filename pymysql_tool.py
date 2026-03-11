#!/usr/bin/python3
# encoding:utf-8
"""
@description: pymsql 工具类
@author: xander
@date: 2021-12-09 10:15:47
"""

# mysql 数据库数字类型

NUMBER_TYPES = ["TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL"]


def __default_val(value, default_value):
    """
    默认值处理
    :param value:
    :param default_value:
    :return:
    """
    return value if value is not None else default_value


def __fetch_field_index_map(c) -> object:
    """
    获取查询表的表头映射表
    :param c: 游标
    :return: 表头映射表
    """
    headers = c.description
    field_map = {}
    for field_info in headers:
        column_index = headers.index(field_info)
        field_map[field_info[0]] = column_index
    return field_map


def __fetch_table_source_data(conn, table_name) -> object:
    """
    获取数据库表结构源数据
    :param conn:
    :param table_name:
    :return:
    """
    database = bytes.decode(conn.__dict__["db"])
    query_sql = " SELECT" \
                " COLUMN_NAME," \
                " COLUMN_COMMENT," \
                " DATA_TYPE " \
                " FROM" \
                " information_schema.COLUMNS " \
                " WHERE" \
                " table_name = '%s' " \
                " AND table_schema = '%s'" % (table_name, database)

    cursor = conn.cursor()
    cursor.execute(query_sql)
    results = cursor.fetchall()
    field_index_map = __fetch_field_index_map(cursor)
    cursor.close()

    list_obj = []
    if results is not None:
        for obj in results:
            list_obj.append(__source_data_to_obj(obj, ["COLUMN_NAME", "COLUMN_COMMENT", "DATA_TYPE"], field_index_map))
    if len(list_obj) < 1:
        raise RuntimeError("操作或查询的表（{}）不存在".format(table_name))
    return list_obj


def __fetch_table_source_data_to_field_map(conn, table_name) -> object:
    """
    获取数据库表结构源数据以字段名称作为key的map
    :param conn:
    :param table_name:
    :return:
    """
    table_field_source_data_list = __fetch_table_source_data(conn, table_name)
    if table_field_source_data_list is not None:
        source_data_map = {}
        for table_field_source_data in table_field_source_data_list:
            source_data_map[table_field_source_data["COLUMN_NAME"]] = table_field_source_data
        return source_data_map
    else:
        return {}


def __source_data_to_obj(source_data, fields, field_index_map) -> object:
    """
    将查询原始数据转为对象
    :param source_data: 原始数据
    :param fields: 映射字段
    :param field_index_map: 原始数据映射表
    :return: 映射之后的对象
    """
    obj = {}
    for field in fields:
        if field in field_index_map:
            field_index = field_index_map[field]
            obj[field] = source_data[field_index]
    return obj


def __generate_insert_sql_template(table_name, obj):
    """
    生成insert sql 模板语句
    :param table_name:
    :param obj:
    :return:
    """
    if table_name is None or obj is None:
        return None

    insert_fields_templates = []
    fields = obj.keys()
    for key in fields:
        insert_fields_templates.append("%s")
    fields_rebuild = list(map(lambda f: "`{}`".format(f), obj.keys()))
    insert_fields_sql = ", ".join(fields_rebuild)
    insert_values_sql = ", ".join(insert_fields_templates)
    insert_sql_template = "INSERT INTO {} ({}) VALUES ({})".format(table_name, insert_fields_sql, insert_values_sql)

    return fields, insert_sql_template


def __generate_update_sql_template(table_name, obj, ignore_fields=[]):
    """
    生成update sql 模板语句
    :param table_name:
    :param obj:
    :param ignore_fields:
    :return:
    """
    if table_name is None or obj is None:
        return None

    fields = obj.keys()
    update_fields_templates = []
    update_fields = []
    for key in fields:
        if key not in ignore_fields:
            update_fields_templates.append("`{}`=%s".format(key))
            update_fields.append(key)
    update_sql_template = "UPDATE {} SET {} ".format(table_name, ", ".join(update_fields_templates))

    return update_fields, update_sql_template


def __fill_list(str, size) -> list:
    """
    使用指定字符串快速填充并生成数组
    :param str:
    :param size:
    :return:
    """
    list = []
    for i in range(size):
        list.append(str)
    return list


def fetch_table_fields(conn, table_name) -> list:
    """
    获取表字段集合
    :param conn: 数据库连接
    :param table_name: 表名
    :return:
    """
    table_field_source_data_list = __fetch_table_source_data(conn, table_name)
    if table_field_source_data_list is not None:
        table_fields = []
        for table_field_source_data in table_field_source_data_list:
            table_fields.append(table_field_source_data["COLUMN_NAME"])
        return table_fields
    else:
        return []


def fetch_table_fields_fast(cursor) -> list:
    """
    获取表字段集合
    :param cursor: 游标
    :return:
    """
    if cursor is not None:
        cols = cursor.description
        table_fields = []
        for col in cols:
            table_fields.append(col[0])
        return table_fields
    else:
        return []
# ########################################################################## 以上为支持代码 #####################################################################################


def execute_sql(conn, exe_sql, params=None, auto_commit=False, auto_close_cursor=False):
    """
    执行SQL
    :param conn: 数据库连接
    :param exe_sql: 执行SQL
    :param params: 占位参数集合
    :param auto_commit: 是否自动提交，一般更新或删除时使用（默认为：False）-可选
    :param auto_close_cursor: 是否自动关闭游标，（默认为：False）-可选
    :return:
    """
    if exe_sql is None:
        return False
    cursor = conn.cursor()
    try:
        cursor.execute(exe_sql, params)
        if auto_commit:
            conn.commit()
    finally:
        if auto_close_cursor:
            cursor.close()
    return cursor


def insert_one(conn, table_name, obj, commit_callback=None) -> bool:
    """
    单条插入
    :param conn: 数据库连接
    :param table_name: 表名
    :param obj: insert的对象（Id需要自行生成）
    :param commit_callback: 手动commit回调（commit_callback(conn)）-可选
    :return:
    """

    if table_name is None or obj is None:
        return False

    try:
        fields, insert_sql_template = __generate_insert_sql_template(table_name, obj)
        if insert_sql_template is not None:
            cursor = conn.cursor()
            insert_data = list(map(lambda field: obj[field], fields))
            cursor.execute(insert_sql_template, insert_data)
            if commit_callback is None:
                # 提交回调为空时使用自动提交
                conn.commit()
            else:
                commit_callback(conn)
            return True
    except BaseException as e:
        conn.rollback()
        print(e)
        raise e


def insert_batch(conn, table_name, obj_list, commit_callback=None) -> bool:
    """
    批量插入
    :param conn: 数据连接
    :param table_name: 表名
    :param obj_list: insert的对象集合（Id需要自行生成）
    :param commit_callback: 手动commit回调（commit_callback(conn)）-可选
    :return:
    """
    if table_name is None or obj_list is None:
        return False

    insert_data_list = []
    fields, insert_sql_template = __generate_insert_sql_template(table_name, obj_list[0])
    for obj in obj_list:
        insert_data_list.append(list(map(lambda field: obj[field], fields)))

    try:
        cursor = conn.cursor()
        cursor.executemany(insert_sql_template, insert_data_list)
        if commit_callback is None:
            # 提交回调为空时使用自动提交
            conn.commit()
        else:
            commit_callback(cursor)
        return True
    except BaseException as e:
        conn.rollback()
        return False


def remove_by_id(conn, table_name, id, id_field="id", commit_callback=None) -> bool:
    """
    根据Id删除数据
    :param conn: 数据库连接
    :param table_name: 表名
    :param id: 删除Id
    :param id_field: id字段名称（默认为为："id"）-可选
    :param commit_callback: 手动commit回调（commit_callback(conn)）-可选
    :return:
    """
    if id is None or len(id) < 1:
        return False

    try:
        del_sql = "DELETE FROM {} WHERE {} = %s".format(table_name, id_field)
        cursor = conn.cursor()
        cursor.execute(del_sql, id)
        if commit_callback is None:
            # 提交回调为空时使用自动提交
            conn.commit()
        else:
            commit_callback(cursor)
        return True
    except BaseException as e:
        conn.rollback()
        print(e)
        return False


def remove_by_ids(conn, table_name, ids, id_field="id", commit_callback=None) -> bool:
    """
    根据Id集合删除数据
    :param conn: 数据库连接
    :param table_name: 表名
    :param ids: 删除Id集合
    :param id_field: id字段名称（默认为为："id"）-可选
    :param commit_callback: 手动commit回调（commit_callback(conn)）-可选
    :return:
    """
    if ids is None or len(ids) < 1:
        return False

    try:
        placeholder_list = __fill_list("%s", len(ids))
        del_sql = "DELETE FROM {} WHERE {} in ({})".format(table_name, id_field, ", ".join(placeholder_list))
        cursor = conn.cursor()
        cursor.execute(del_sql, ids)
        if commit_callback is None:
            # 提交回调为空时使用自动提交
            conn.commit()
        else:
            commit_callback(cursor)
        return True
    except BaseException as e:
        conn.rollback()
        print(e)
        return False


def update_by_id(conn, table_name, obj, id_field="id", commit_callback=None) -> bool:
    """
    根据Id更新单条数据
    :param conn: 数据库连接
    :param table_name: 表名
    :param obj: 更新数据对象（需要携带Id）
    :param id_field: id字段名称（默认为为："id"）-可选
    :param commit_callback: 手动commit回调（commit_callback(conn)）-可选
    :return:
    """
    if obj is None:
        return False

    try:
        update_field_templates = []
        update_field_values = []
        for field in obj.keys():
            if id_field != field:
                update_field_values.append(obj[field])
                update_field_templates.append("{}=%s".format(field))
        update_sql_template = "UPDATE {} SET {} WHERE {} = %s".format(table_name, ", ".join(update_field_templates),
                                                                      id_field)
        cursor = conn.cursor()
        update_field_values.append(obj[id_field])
        cursor.execute(update_sql_template, update_field_values)
        if commit_callback is None:
            # 提交回调为空时使用自动提交
            conn.commit()
        else:
            commit_callback(cursor)
        return True
    except BaseException as e:
        conn.rollback()
        print(e)
        return False


def update_batch_by_id(conn, table_name, obj_list, id_field="id", commit_callback=None) -> bool:
    """
    批量更新
    :param conn: 数据库连接
    :param table_name: 表名
    :param obj_list: 更新数据对象集合（需要携带Id）
    :param id_field: id字段名称（默认为为："id"）-可选
    :param commit_callback: 手动commit回调（commit_callback(conn)）-可选
    :return:
    """
    if obj_list is None:
        return False

    try:
        fields, update_sql_template = __generate_update_sql_template(table_name, obj_list[0], ["id"])
        update_sql_template = update_sql_template + "WHERE {}=%s".format(id_field)
        fields.append(id_field)

        update_field_values = []
        for obj in obj_list:
            update_row_values = []
            for field in fields:
                if id_field != field:
                    update_row_values.append(obj[field])
            update_row_values.append(obj[id_field])
            update_field_values.append(update_row_values)

        cursor = conn.cursor()
        cursor.executemany(update_sql_template, update_field_values)
        if commit_callback is None:
            # 提交回调为空时使用自动提交
            conn.commit()
        else:
            commit_callback(cursor)
        return True
    except BaseException as e:
        conn.rollback()
        print(e)
        return False


def select_one(conn, exe_sql, fields=None, params=None, table_name=None) -> object:
    """
    单对象查询
    :param conn: 数据库连接
    :param exe_sql: 执行SQL
    :param fields: 查询返回字段（默认表的全字段）-可选
    :param params: 占位参数集合-可选
    :param table_name: 占位参数表名-可选
    :return: 查询对象
    """
    if exe_sql is None:
        return None

    cursor = execute_sql(conn, exe_sql, params)
    result = cursor.fetchone()

    if result is None:
        return None
    if fields is None:
        fields = fetch_table_fields_fast(cursor)

    field_index_map = __fetch_field_index_map(cursor)
    cursor.close()

    return __source_data_to_obj(result, fields, field_index_map)


def select_list(conn, exe_sql, fields=None, params=None, table_name=None) -> list:
    """
    列表查询
    :param conn: 数据库连接
    :param exe_sql: 执行SQL
    :param fields: 提供查询返回字段，返回指定字段
    :param params: 占位集合参数
    :param table_name: 提供查询表名，则返回所有字段
    :return: 数据集合
    """
    if exe_sql is None:
        return None
    cursor = execute_sql(conn, exe_sql, params)
    results = cursor.fetchall()
    field_index_map = __fetch_field_index_map(cursor)

    if fields is None:
        fields = fetch_table_fields_fast(cursor)

    cursor.close()

    list_obj = []
    if results is not None:
        for obj in results:
            list_obj.append(__source_data_to_obj(obj, fields, field_index_map))

    return list_obj


def select_page(conn, exe_sql, page_number, page_size, fields=None, params=None) -> list:
    """
    列表查询
    :param conn: 数据库连接
    :param exe_sql: 执行SQL
    :param page_number: 分页数
    :param page_size: 分页数量
    :param fields: 提供查询返回字段，返回指定字段
    :param params: 占位集合参数
    :param table_name: 提供查询表名，则返回所有字段
    :return: 数据集合
    """
    count_sql = "SELECT COUNT(1) FROM ({}) AS tmp".format(exe_sql)
    total_count = select_simple_value(conn, count_sql, 0, params)
    total_page = int(total_count / page_size)
    if total_count % page_size > 0:
        total_page += 1
    begin_index = (page_number - 1) * page_size
    page_sql = "SELECT tmp.* FROM ({}) AS tmp LIMIT {},{}".format(exe_sql, begin_index, page_size)

    page = select_list(conn, page_sql, fields, params, None)
    result = {
        "pageNumber": page_number,
        "pageSize": page_size,
        "totalPage": total_page,
        "totalCount": total_count,
        "page": page
    }
    return result


def select_simple_value(conn, exe_sql, or_default_value=None, params=None):
    """
    查询返回单值
    :param conn: 数据库连接
    :param exe_sql: 执行Sql
    :param or_default_value: 查询值为空时处理值
    :param params: 占位参数值集合
    :return:
    """
    if exe_sql is None:
        return 0
    cursor = conn.cursor()
    if params is not None and len(params) > 0:
        cursor.execute(exe_sql, params)
    else:
        cursor.execute(exe_sql)

    source_data = cursor.fetchone()
    if source_data is not None and (len(source_data) > 0):
        return __default_val(source_data[0], or_default_value)
    else:
        return None


class SqlBuilder:

    def __init__(self, conn, table_name):
        self.__conn = conn
        self.__table_name = table_name
        self.__column_names = []
        self.__wheres = []
        self.__parent_group = None
        self.__group = None
        self.__sets = []
        self.__order_bys = []

    def _append_where(self, column_name, value, column_symbol, logic_symbol=None, multi_value=False):
        self.__wheres.append({"column_name": column_name,
                            "value": value,
                            "column_symbol": column_symbol,
                            "column_end": "",
                            "logic_symbol": logic_symbol,
                            "multi_value": multi_value})
        return self

    def _append_set(self, column_name, value):
        self.__sets.append({"column_name": column_name,
                          "value": value,
                          "column_symbol": "=",
                          "column_end": ",",
                          "logic_symbol": None,
                          "multi_value": False})
        return self

    def _append_oder(self, column_name, column_symbol):
        self.__order_bys.append({"column_name": column_name,
                               "value": None,
                               "column_symbol": column_symbol,
                               "column_end": ",",
                               "logic_symbol": None,
                               "multi_value": False})
        return self

    @staticmethod
    def _trim_where_excess_logic_symbol(sql):
        """
        去除Where语句多余的逻辑符号
        :param sql:
        :return:
        """
        import re
        sql = sql.lstrip()
        sql = re.sub("\(\s+AND", "(", sql)
        sql = re.sub("\(\s+OR", "(", sql)
        pattern = re.compile(r"(?:AND|OR)?([\s\S]+)")
        match = pattern.match(sql)
        sql = match.group(1)
        return " {}".format(sql)

    @staticmethod
    def _trim_set_excess_logic_symbol(sql):
        """
        去除Set语句多余的逻辑符号
        :param sql:
        :return:
        """
        sql = sql.strip()
        if sql.endswith(","):
            sql = sql[0:len(sql) - 1]
        return sql

    def group_(self, filter_callback):
        self._append_where(None, None, "( ")
        filter_callback(self)
        self._append_where(None, None, " )")
        return self

    def and_(self):
        self._append_where(None, None, None, "AND")
        return self

    def or_(self):
        self._append_where(None, None, None, "OR")
        return self

    def not_(self):
        self._append_where(None, None, None, "NOT")
        return self

    def in_(self):
        self._append_where(None, None, "IN")
        return self

    def not_in_(self):
        self.not_().in_()
        return self

    def lt_(self, column_name, value):
        self._append_where(column_name, value, "<")
        return self

    def le_(self, column_name, value):
        self._append_where(column_name, value, "<=")
        return self

    def gt_(self, column_name, value):
        self._append_where(column_name, value, ">")
        return self

    def ge_(self, column_name, value):
        self._append_where(column_name, value, ">=")
        return self

    def eq_(self, column_name, value):
        self._append_where(column_name, value, "=")
        return self

    def not_eq_(self, column_name, value):
        self._append_where(column_name, value, "!=")
        return self

    def like_(self, column_name, value):
        self._append_where(column_name, value, "LIKE")
        return self

    def not_like_(self, column_name, value):
        self._append_where(column_name, value, "NOT LIKE")
        return self

    def between_(self, column_name, begin_value, end_value):
        self._append_where(column_name, None, "BETWEEN") \
            ._append_where(None, begin_value, None) \
            .and_() \
            ._append_where(None, end_value, None)
        return self

    def not_between_(self, column_name, begin_value, end_value):
        self._append_where(column_name, None, "NOT BETWEEN") \
            .not_() \
            ._append_where(None, begin_value, None) \
            .and_() \
            ._append_where(None, end_value, None)
        return self

    def and_eq_(self, condition, column_name, value):
        if condition:
            self.and_().eq_(column_name, value)
        return self

    def and_lt_(self, condition, column_name, value):
        if condition:
            self.and_().lt_(column_name, value)
        return self

    def and_le_(self, condition, column_name, value):
        if condition:
            self.and_().le_(column_name, value)
        return self

    def and_gt_(self, condition, column_name, value):
        if condition:
            self.and_().gt_(column_name, value)
        return self

    def and_ge_(self, condition, column_name, value):
        if condition:
            self.and_().ge_(column_name, value)
        return self

    def and_not_eq_(self, condition, column_name, value):
        if condition:
            self.and_().not_eq_(column_name, value)
        return self

    def and_in_(self, condition, column_name, values):
        if condition:
            self.and_()\
                ._append_where(column_name, None, None)\
                .in_()\
                ._append_where(None, values, None, multi_value=True)
        return self

    def and_like_(self, condition, column_name, values):
        if condition:
            self.and_().like_(column_name, values)
        return self

    def and_not_like_(self, condition, column_name, values):
        if condition:
            self.and_().not_like_(column_name, values)
        return self

    def and_not_in_(self, condition, column_name, values):
        if condition:
            self.and_() \
                ._append_where(column_name, None, None) \
                .not_in_() \
                ._append_where(None, values, None, multi_value=True)
        return self

    def and_between_(self, condition, column_name, begin_value, end_value):
        if condition:
            self.and_().between_(column_name, begin_value, end_value)
        return self

    def and_not_between_(self, condition, column_name, begin_value, end_value):
        if condition:
            self.and_().not_between_(column_name, begin_value, end_value)
        return self

    def or_eq_(self, condition, column_name, value):
        if condition:
            self.or_().eq_(column_name, value)
        return self

    def or_lt_(self, condition, column_name, value):
        if condition:
            self.and_().lt_(column_name, value)
        return self

    def or_le_(self, condition, column_name, value):
        if condition:
            self.and_().le_(column_name, value)
        return self

    def or_gt_(self, condition, column_name, value):
        if condition:
            self.and_().gt_(column_name, value)
        return self

    def or_ge_(self, condition, column_name, value):
        if condition:
            self.and_().ge_(column_name, value)
        return self

    def or_not_eq_(self, condition, column_name, value):
        if condition:
            self.or_().not_eq_(column_name, value)
        return self

    def or_in_(self, condition, column_name, values):
        if condition:
            self.or_() \
                ._append_where(column_name, None, None) \
                .in_() \
                ._append_where(None, values, None, multi_value=True)
        return self

    def or_not_in_(self, condition, column_name, values):
        if condition:
            self.or_() \
                ._append_where(column_name, None, None) \
                .not_() \
                ._append_where(None, values, None, multi_value=True)
        return self

    def or_like_(self, condition, column_name, values):
        if condition:
            self.or_().like_(column_name, values)
        return self

    def or_not_like_(self, condition, column_name, values):
        if condition:
            self.or_().not_like_(column_name, values)
        return self

    def or_between_(self, condition, column_name, begin_value, end_value):
        if condition:
            self.or_().between_(column_name, begin_value, end_value)
        return self

    def or_not_between_(self, condition, column_name, begin_value, end_value):
        if condition:
            self.or_().not_between_(column_name, begin_value, end_value)
        return self

    def set_(self, condition, column_name, value):
        if condition:
            self._append_set(column_name, value)
        return self

    def desc_(self, condition, *column_names):
        if condition:
            sql_part = ", ".join(column_names)
            self._append_oder(sql_part, "DESC")
        return self

    def asc_(self, condition, *column_names):
        if condition:
            sql_part = ", ".join(column_names)
            self._append_oder(sql_part, "ASC")
        return self

    @staticmethod
    def get_part_sql_template(source_data_parts):
        sql_template = ""
        column_values = []
        if source_data_parts is not None and len(source_data_parts) > 0:
            for source_data_part in source_data_parts:
                column_name = source_data_part["column_name"]
                column_name = "`{}`".format(column_name) if column_name is not None else ""
                column_symbol = source_data_part["column_symbol"]
                column_symbol = column_symbol if column_symbol is not None else ""
                logic_symbol = source_data_part["logic_symbol"]
                logic_symbol = logic_symbol if logic_symbol is not None else ""
                column_end = source_data_part["column_end"]
                column_end = column_end if column_end is not None else ""
                multi_value = source_data_part["multi_value"]
                multi_value = multi_value if multi_value is not None else False
                value = source_data_part["value"]

                value_placeholder = ""
                if value is not None:
                    value_size = len(value) if multi_value else 1
                    value_placeholder = ("%s, " * value_size)[0:4 * value_size - 2]
                    value_placeholder = "({})".format(value_placeholder) if multi_value else value_placeholder
                    value_placeholder += column_end

                sql_template += " {} {} {} {} ".format(logic_symbol, column_name, column_symbol, value_placeholder)

                # 构建参数
                if value is not None:
                    if multi_value:
                        for val in value:
                            column_values.append(val)
                    else:
                        column_values.append(value)
        return sql_template, column_values

    def del_(self):
        where_sql, column_values2 = self.get_part_sql_template(self.__wheres)
        where_sql = self._trim_where_excess_logic_symbol(where_sql)
        sql_template = "DELETE FROM {} \n" \
                       "WHERE \n {}".format(self.__table_name, where_sql)
        execute_sql(self.__conn, sql_template, column_values2, True, True)
        return True

    def update_(self) -> bool:
        set_sql, column_values1 = self.get_part_sql_template(self.__sets)
        set_sql = self._trim_set_excess_logic_symbol(set_sql)
        where_sql, column_values2 = self.get_part_sql_template(self.__wheres)
        where_sql = self._trim_where_excess_logic_symbol(where_sql)
        sql_template = "UPDATE {} \n " \
                       "SET {} \n" \
                       "WHERE \n {}".format(self.__table_name, set_sql, where_sql)

        values = column_values1 + column_values2
        execute_sql(self.__conn, sql_template, values, True, True)
        return True

    def _get_select_sql_template(self, column_names):
        column_names = column_names if len(column_names) > 0 else fetch_table_fields(self.__conn, self.__table_name)
        column_names_rebuild = list(map(lambda column_name: "`"+column_name+"`", column_names))
        column_name_sql = ",".join(column_names_rebuild)
        where_sql, column_values1 = self.get_part_sql_template(self.__wheres)
        order_sql, column_values2 = self.get_part_sql_template(self.__order_bys)
        sql_template = "SELECT \n" \
                       " {} \n" \
                       "FROM {} \n".format(column_name_sql, self.__table_name)
        if len(where_sql) > 0:
            where_sql = self._trim_where_excess_logic_symbol(where_sql)
            sql_template += "WHERE \n {}".format(where_sql)
        if len(order_sql) > 0:
            sql_template += "ORDER BY {}".format(order_sql)
        return sql_template, column_names, column_values1

    def select_list_(self, *column_names):
        sql_template, column_names, column_values1 = self._get_select_sql_template(column_names)
        return select_list(self.__conn, sql_template, column_names, column_values1)

    def select_page_(self, page_number, page_size, *column_names):
        sql_template, column_names, column_values1 = self._get_select_sql_template(column_names)
        total_count = self.count_()
        total_page = int(total_count/page_size)
        if total_count%page_size > 0:
            total_page += 1
        begin_index = (page_number - 1) * page_size
        sql_template += " LIMIT {},{}".format(begin_index, page_size)
        page = select_list(self.__conn, sql_template, column_names, column_values1)
        result = {
            "pageNumber": page_number,
            "pageSize": page_size,
            "totalPage": total_page,
            "totalCount": total_count,
            "page": page
        }
        return result

    def select_one_(self, *column_names):
        sql_template, column_names, column_values1 = self._get_select_sql_template(column_names)
        return select_one(self.__conn, sql_template, column_names, column_values1)

    def count_(self):
        pass
        where_sql, column_values1 = self.get_part_sql_template(self.__wheres)
        sql_template = "SELECT \n" \
                       " COUNT(1) \n" \
                       "FROM {} \n".format(self.__table_name)
        if len(where_sql) > 0:
            where_sql = self._trim_where_excess_logic_symbol(where_sql)
            sql_template += "WHERE \n {}".format(where_sql)

        return select_simple_value(self.__conn, sql_template, 0, column_values1)
