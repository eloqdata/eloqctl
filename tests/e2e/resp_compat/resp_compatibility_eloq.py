#!/usr/bin/env python3

import runpy
import ssl

import redis


_ORIGINAL_REDIS = redis.Redis
_ORIGINAL_REDIS_CLUSTER = redis.RedisCluster


class EloqRedis(_ORIGINAL_REDIS):
    def __init__(self, *args, **kwargs):
        if kwargs.get("ssl"):
            kwargs.setdefault("ssl_cert_reqs", ssl.CERT_NONE)
            kwargs.setdefault("ssl_check_hostname", False)
            kwargs.setdefault("protocol", 2)
        super().__init__(*args, **kwargs)


class EloqRedisCluster(_ORIGINAL_REDIS_CLUSTER):
    def __init__(self, *args, **kwargs):
        if kwargs.get("ssl"):
            kwargs.setdefault("ssl_cert_reqs", ssl.CERT_NONE)
            kwargs.setdefault("ssl_check_hostname", False)
        kwargs.setdefault("protocol", 2)
        super().__init__(*args, **kwargs)


redis.Redis = EloqRedis
redis.RedisCluster = EloqRedisCluster

runpy.run_path("/opt/resp-compatibility/resp_compatibility.py", run_name="__main__")
