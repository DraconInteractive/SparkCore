"""Disposable real-nginx deployment fixture; never installs or changes OS services."""
from http.client import HTTPSConnection, HTTPConnection
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import time


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class RemoteProxy:
    def __init__(self, root, config):
        self.process = None
        self.root = root
        self.http = free_port()
        self.tcp = free_port()
        self.host = 'localhost:' + str(self.http)
        self.origin = 'https://' + self.host
        packet = json.loads(config.read_text())
        self.backends = (packet['server']['port'], packet['server']['notification_port'])
        packet['server']['tls_notification_port'] = self.tcp
        packet['auth']['browser_origin'] = self.origin
        config.write_text(json.dumps(packet))
        self.cert = root / 'cert.pem'
        key = root / 'key.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
                        '-keyout', str(key), '-out', str(self.cert)], check=True, capture_output=True)
        self.context = ssl.create_default_context(cafile=str(self.cert))
        self.config = root / 'nginx.conf'
        subprocess.run(['python3', 'scripts/configure-remote.py', '--config', str(config),
                        '--certificate', str(self.cert), '--key', str(key),
                        '--stream-module', os.environ.get('SPARK_TEST_NGINX_MODULE', '/usr/lib/nginx/modules/ngx_stream_module.so'),
                        '--runtime', str(root / 'proxy-runtime'), '--output', str(self.config)], check=True)
        self.command = [os.environ.get('SPARK_TEST_NGINX', '/usr/sbin/nginx'), '-c', str(self.config)]
        subprocess.run(self.command + ['-t'], check=True, capture_output=True)

    def start(self):
        self.process = subprocess.Popen(self.command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            if self.process.poll() is not None:
                raise AssertionError('nginx exited before listening')
            try:
                with self.context.wrap_socket(socket.create_connection(('127.0.0.1', self.http), timeout=1), server_hostname='localhost'):
                    return
            except OSError:
                time.sleep(.05)
        raise AssertionError('nginx did not start')

    def close(self):
        if self.process:
            self.process.terminate()
            self.process.wait(timeout=10)

    def connection(self):
        return HTTPSConnection('localhost', self.http, context=self.context, timeout=5)

    def peer(self):
        return self.context.wrap_socket(socket.create_connection(('127.0.0.1', self.tcp), timeout=5), server_hostname='localhost')

    def check_transport(self):
        # Inspect the actual IPv4 and IPv6 listeners, not just the config file.
        listeners = []
        for table in ('/proc/net/tcp', '/proc/net/tcp6'):
            for row in Path(table).read_text().splitlines()[1:]:
                fields = row.split()
                if fields[3] == '0A':
                    address, port = fields[1].split(':')
                    if int(port, 16) in self.backends:
                        listeners.append((int(port, 16), address))
        assert sorted(listeners) == sorted((port, '0100007F') for port in self.backends), listeners
        for port in (self.http, self.tcp):
            # Neither TLS front door accepts application plaintext.
            with socket.create_connection(('127.0.0.1', port), timeout=3) as sock:
                sock.sendall(b'GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
                try: data = sock.recv(4096)
                except ConnectionResetError: data = b''
                assert b'200 OK' not in data and b'"status"' not in data and b'welcome' not in data
            # Untrusted certificates and incorrect hostnames are refused by clients.
            for context, hostname in ((ssl.create_default_context(), 'localhost'), (self.context, 'wrong.example')):
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=3) as sock:
                        with context.wrap_socket(sock, server_hostname=hostname):
                            raise AssertionError('Certificate verification was bypassed')
                except ssl.SSLCertVerificationError:
                    pass
            with self.context.wrap_socket(socket.create_connection(('127.0.0.1', port), timeout=3), server_hostname='localhost') as sock:
                assert sock.version() in ('TLSv1.2', 'TLSv1.3')
        c = self.connection()
        c.request('GET', '/health', headers={'Host': 'evil.example', 'X-Forwarded-Host': self.host})
        r = c.getresponse(); assert r.status == 421; r.read(); c.close()
        # Forged forwarding headers never create an identity.
        c = self.connection()
        c.request('GET', '/health', headers={'Forwarded': 'proto=https;host='+self.host, 'X-Forwarded-For': '127.0.0.1'})
        r = c.getresponse(); assert r.status == 401; r.read(); c.close()

    def browser_session(self, token):
        def request(method, path, body=None, headers=None, status=200):
            c = self.connection()
            h = {'Content-Type': 'application/json', 'Origin': self.origin, **(headers or {})}
            c.request(method, path, json.dumps(body) if body is not None else None, h)
            r = c.getresponse(); data = r.read(); response_headers = dict(r.getheaders()); c.close()
            assert r.status == status, (path, r.status, data[:200])
            assert response_headers.get('Strict-Transport-Security') == 'max-age=31536000'
            return (json.loads(data) if data and response_headers.get('Content-Type', '').startswith('application/json') else data), response_headers
        packet, headers = request('POST', '/auth/login', {'token': token}, {'X-SPARK-Login': '1'})
        cookie = headers['Set-Cookie'].split(';')[0]
        assert all(flag in headers['Set-Cookie'] for flag in ('Secure', 'HttpOnly', 'SameSite=Strict'))
        session_headers = {'Cookie': cookie}
        for page in ('/', '/api/docs', '/files/explorer', '/projects/explorer', '/commands/explorer'):
            request('GET', page, headers=session_headers)
        request('POST', '/files/directories', {'directory': '/tls-browser'}, session_headers, 403)
        session_headers['X-SPARK-CSRF'] = packet['csrf_token']
        request('POST', '/files/directories', {'directory': '/tls-browser'}, session_headers)
        request('POST', '/files/directories', {'directory': '/tls-denied'}, {**session_headers, 'Origin': 'https://evil.example'}, 403)
        return request, session_headers
