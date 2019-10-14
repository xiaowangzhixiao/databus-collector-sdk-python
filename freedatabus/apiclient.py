import requests
import time
import hashlib

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
