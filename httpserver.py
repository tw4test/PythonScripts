import http.server
import ssl
import urllib.parse
import json
import logging
import socketserver
import os
import threading
import time
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_selfsigned_cert(certfile="server.pem"):
    if not os.path.exists(certfile):
        try:
            result = subprocess.run([
                'openssl', 'req', '-x509', '-newkey', 'rsa:4096', 
                '-keyout', certfile, '-out', certfile, 
                '-days', '365', '-nodes', '-subj', '/CN=localhost'
            ], check=True, capture_output=True, text=True)
            logging.info("Self-signed certificate generated successfully")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to generate certificate: {e.stderr}")
            raise
        except FileNotFoundError:
            logging.error("OpenSSL not found. Please install OpenSSL to generate certificates.")
            raise

class RequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.log_request(200)
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.send_header('Content-Length', '2')
        self.end_headers()
        self.wfile.write(b'OK')
        self.wfile.flush()

    def do_POST(self):
        self.log_request(200)
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.send_header('Content-Length', '2')
        self.end_headers()
        self.wfile.write(b'OK')
        self.wfile.flush()

    def log_request(self, code='-'):
        logging.info(f"Request: {self.command} {self.path} {self.protocol_version}")
        logging.info(f"Response Code: {code}")
        parsed_url = urllib.parse.urlparse(self.path)
        logging.info(f"URL: {parsed_url.path}")
        query_params = urllib.parse.parse_qs(parsed_url.query)
        if query_params:
            logging.info(f"Params: {query_params}")
        if self.command == 'POST':
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length:
                body = self.rfile.read(content_length).decode('utf-8')
                try:
                    json_body = json.loads(body)
                    logging.info(f"Body (JSON): {json_body}")
                except json.JSONDecodeError:
                    logging.info(f"Body (Plain Text): {body}")

def run_server(server, protocol="HTTP"):
    logging.info(f"Starting {protocol} server on port {server.server_address[1]}")
    server.timeout = 1  # Set timeout to allow periodic checking of shutdown
    try:
        server.serve_forever()
    except Exception as e:
        logging.error(f"{protocol} server error: {e}")
    finally:
        server.server_close()
        logging.info(f"{protocol} server stopped")

if __name__ == "__main__":
    try:
        generate_selfsigned_cert()
    except Exception as e:
        logging.error("Failed to start servers due to certificate generation error")
        exit(1)

    try:
        httpd = socketserver.ThreadingTCPServer(("", 8080), RequestHandler)
        httpsd = socketserver.ThreadingTCPServer(("", 8443), RequestHandler)
        
        # Configure HTTPS
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain("server.pem")
        httpsd.socket = context.wrap_socket(httpsd.socket, server_side=True)
    except Exception as e:
        logging.error(f"Failed to create servers: {e}")
        exit(1)

    http_thread = threading.Thread(target=run_server, args=(httpd, "HTTP"))
    https_thread = threading.Thread(target=run_server, args=(httpsd, "HTTPS"))

    # Don't set as daemon threads - we want explicit control over shutdown
    # http_thread.daemon = True
    # https_thread.daemon = True

    http_thread.start()
    https_thread.start()

    try:
        while http_thread.is_alive() or https_thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down servers...")
        
        # Initiate shutdown
        httpd.shutdown()
        httpsd.shutdown()
        
        # Close servers
        httpd.server_close()
        httpsd.server_close()
        
        # Wait for threads to finish with timeout
        http_thread.join(timeout=5)
        https_thread.join(timeout=5)
        
        if http_thread.is_alive() or https_thread.is_alive():
            logging.warning("Some server threads did not terminate cleanly")
        else:
            logging.info("Servers stopped cleanly")
