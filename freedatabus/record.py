import json
import time
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
                apiclient.regist_schema(topic, schema)
            schema_dict[key] = schema
        else:
            if topic not in schema_dict:
                apiclient.regist_schema(topic, default_schema)
            schema_dict[key] = default_schema

        self.topic = topic
        self.key = key
        self.value = value

    def to_json(self):
        tmp = {}
        tmp["key"] = self.key
        tmp["value"] = obj_dic = class2dic(self.value)
        return str(json.dumps(tmp))


if __name__ == "__main__":
    r = Record("sensor-test", "aaa",
               {"value": 5, "createdTime": int(round(time.time() * 1000))})
    print(r.to_json())


class SensorData(object):
    def __init__(self, createdTime, value, name=None):
        self.createdTime = createdTime
        self.value = value
        if name is not None:
            self.name = name
