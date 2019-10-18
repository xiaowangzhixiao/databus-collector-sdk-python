import json
import time
import requests
import time
import hashlib

default_schema = """
{
    \"name\": \"sensor\",
    \"type\": \"record\",
    \"fields\": [
        {\"name\":\"name\",\"type\": [\"string\", \"null\"]},
        {\"name\":\"value\", \"type\": \"double\"},
        {\"name\":\"createdTime\", \"type\": \"long\"}
    ]
}
"""


def json_serialize(obj):
    obj_dic = class2dic(obj)
    return json.dumps(obj_dic)


def class2dic(obj):
    obj_dic = obj.__dict__
    for key in obj_dic.keys():
        value = obj_dic[key]
        obj_dic[key] = value2py_data(value)
    return obj_dic


def value2py_data(value):
    if str(type(value)).__contains__('.'):
        # value 为自定义类
        value = class2dic(value)
    elif str(type(value)) == "<class 'list'>":
        # value 为列表
        for index in range(0, value.__len__()):
            value[index] = value2py_data(value[index])
    return value

schema_dict = {}


class Record(object):
    def __init__(self, topic, key: str, value: object, schema=None):
        if schema is not None:
            if topic not in schema_dict:
                regist_schema(topic, schema)
            schema_dict[key] = schema
        else:
            if topic not in schema_dict:
                regist_schema(topic, default_schema)
            schema_dict[key] = default_schema

        self.topic = topic
        self.key = key
        self.value = value

    def to_json(self):
        tmp = {}
        tmp["key"] = self.key
        tmp["value"] = obj_dic = class2dic(self.value)
        return str(json.dumps(tmp))


class SensorData(object):
    def __init__(self, createdTime, value, name=None):
        self.createdTime = createdTime
        self.value = value
        if name is not None:
            self.name = name


appkey = 'pi'
secretkey = 'abc123'

api_url = "http://databus-api.free4inno.com"


def get(url, params={}):
    t = int(time.time() * 1000)
    sign = get_sign(appkey, secretkey, t)
    params['appkey'] = appkey
    params['timestamp'] = t
    params['sign'] = sign
    return requests.get(url, params)


def post(url, data={}):
    t = int(time.time() * 1000)
    sign = get_sign(appkey, secretkey, t)
    data['appkey'] = appkey
    data['timestamp'] = t
    data['sign'] = sign
    return requests.post(url, data)


def get_sign(appkey, secretkey, t):
    tc = str(t)
    sign = ''
    m = hashlib.md5()
    m.update(sign.encode('utf8'))
    sign = m.hexdigest()
    m = hashlib.md5()
    m.update((sign + appkey + secretkey + tc).encode('utf8'))
    sign = m.hexdigest()
    return sign


def regist_schema(topic, schema):
    r = post(api_url + "/topics/" + topic + "/schema", data={"schema": schema})
    print(r.content)


def get_schema(topic):
    r = get(api_url + "/topics/" + topic + "/schema")
    print(r.content)


if __name__ == "__main__":
    regist_schema("api-test", record.default_schema)
    get_schema("api-test")
