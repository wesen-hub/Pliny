#!/usr/bin/env python3
# 用 AWS SigV4 在 minio 创建 bucket(S3 PutBucket, path-style)
import hashlib, hmac, datetime
import requests

AK = 'minioadmin'
SK = 'minioadmin123'
endpoint = 'http://10.66.0.2:9000'
bucket = 'dcp-lake'
region = 'us-east-1'
service = 's3'


def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def sig_key(secret, date, region, service):
    k = ('AWS4' + secret).encode('utf-8')
    for d in (date, region, service, 'aws4_request'):
        k = sign(k, d)
    return k


now = datetime.datetime.utcnow()
amzdate = now.strftime('%Y%m%dT%H%M%SZ')
datestamp = now.strftime('%Y%m%d')

host = '10.66.0.2:9000'
path = '/' + bucket
payload_hash = hashlib.sha256(b'').hexdigest()

headers = {'host': host, 'x-amz-content-sha256': payload_hash, 'x-amz-date': amzdate}
canonical_headers = ''.join(f'{k}:{v}\n' for k, v in sorted(headers.items()))
signed_headers = ';'.join(sorted(headers.keys()))
canonical_request = 'PUT\n' + path + '\n\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash
credential_scope = f'{datestamp}/{region}/{service}/aws4_request'
string_to_sign = ('AWS4-HMAC-SHA256\n' + amzdate + '\n' + credential_scope + '\n'
                  + hashlib.sha256(canonical_request.encode('utf-8')).hexdigest())
signing_key = sig_key(SK, datestamp, region, service)
signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
authorization = (f'AWS4-HMAC-SHA256 Credential={AK}/{credential_scope}, '
                 f'SignedHeaders={signed_headers}, Signature={signature}')

headers['Authorization'] = authorization
r = requests.put(endpoint + path, headers=headers, timeout=15)
print(r.status_code, r.text[:300])
