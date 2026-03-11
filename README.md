# pymysql_tool
一款基于Python实现封装对mysql数据的操作

## 使用示例
```python
import pandas as pd
import pymysql
import pymysql_tool

if __name__ == '__main__':
    # 创建连接
    db = pymysql.connect(host='10.8.0.2', port=3306, user='root', passwd='123456', db='aobo_large', charset='utf8')

    values = pymysql_tool.SqlBuilder(db, "alarm_msg")\
        .group_(lambda w: w.and_between_(True, "continued_time", 100, 1000000000000000))\
        .and_()\
        .group_(lambda w: w.and_not_eq_(True, "id", 1).and_not_eq_(True, "alarm_config_id", 99)) \
        .and_eq_(True, "alarm_config_id", 1) \
        .and_between_(True, "continued_time", 100, 1000000000000000)\
        .select_list_()

    for v in values:
        print(v)
    print("==========")
    print(len(values))
    print(values)


    db.close()
pass
```
