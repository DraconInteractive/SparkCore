#!/usr/bin/env python3
"""Validate both generated nginx modes with temporary certificates; no listeners."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile

spec=importlib.util.spec_from_file_location('configure_remote',Path(__file__).with_name('configure-remote.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
summary={'remote':True,'auth_mode':'required','browser_origin':'https://spark.dracon.au:48443',
         'server':{'host':'127.0.0.1','port':47683,'notification_port':47684},
         'public_notifications':{'port':47685}}
with tempfile.TemporaryDirectory(prefix='spark-proxy-config-') as tmp:
    root=Path(tmp);cert=root/'cert.pem';key=root/'key.pem'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=spark.dracon.au',
                    '-keyout',str(key),'-out',str(cert)],check=True,capture_output=True)
    stream=Path(os.environ.get('SPARK_TEST_NGINX_MODULE','/usr/lib/nginx/modules/ngx_stream_module.so'))
    nginx=os.environ.get('SPARK_TEST_NGINX','/usr/sbin/nginx')
    for tcp_only in (False,True):
        config=module.render(summary,cert,key,stream,root,tcp_only)
        assert ('http {' in config) is not tcp_only
        assert ('listen 0.0.0.0:48443 ssl;' in config) is not tcp_only
        assert 'listen 0.0.0.0:47685 ssl;' in config
        assert 'listen [::]:47685 ssl ipv6only=on;' in config
        path=root/'nginx.conf';path.write_text(config)
        result=subprocess.run([nginx,'-t','-c',str(path)],capture_output=True,text=True)
        assert result.returncode==0,result.stderr
    try:module.quote('/tmp/unsafe$path')
    except ValueError:pass
    else:raise AssertionError('nginx variable expansion permitted')
print('Combined and TCP-only proxy configurations passed nginx validation; TCP-only has no HTTP listeners.')
