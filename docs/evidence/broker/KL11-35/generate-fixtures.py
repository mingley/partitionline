#!/usr/bin/env python3
"""Generate PUBLIC TEST-ONLY OpenSSL identities; never use these private keys live."""

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


root = Path(__file__).resolve().parents[4]
destination = root / "partitionline-broker/tests/fixtures/tls"
destination.mkdir(parents=True, exist_ok=True)
commands = []
with tempfile.TemporaryDirectory(prefix="partitionline-tls-") as temporary:
    scratch = Path(temporary)

    def run(*arguments):
        command = ["openssl", *arguments]
        commands.append(command)
        subprocess.run(command, cwd=scratch, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def key(name):
        run("ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", f"{name}.key.pem")
        run("pkcs8", "-topk8", "-nocrypt", "-in", f"{name}.key.pem", "-outform", "DER", "-out", f"{name}.key.der")

    def authority(name):
        (scratch / f"{name}.index").touch()
        (scratch / f"{name}.serial").write_text("1000\n")
        (scratch / f"{name}.conf").write_text(f"""[ca]
default_ca = authority
[authority]
database = {name}.index
serial = {name}.serial
new_certs_dir = .
certificate = {name}.cert.pem
private_key = {name}.key.pem
default_md = sha256
policy = names
unique_subject = no
[names]
commonName = supplied
""")

    def certificate(name, ca, eku, expired=False, dns=None, is_ca=False):
        key(name)
        run("req", "-new", "-sha256", "-key", f"{name}.key.pem", "-out", f"{name}.csr", "-subj", f"/CN=KL11-public-test-{name}")
        extension = "basicConstraints=critical,CA:TRUE,pathlen:0\nkeyUsage=critical,keyCertSign,cRLSign\n" if is_ca else f"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage={eku}\n"
        if dns:
            extension += f"subjectAltName=DNS:{dns}\n"
        (scratch / f"{name}.ext").write_text(extension)
        run("ca", "-batch", "-notext", "-config", f"{ca}.conf", "-extfile", f"{name}.ext", "-in", f"{name}.csr", "-out", f"{name}.cert.pem", "-startdate", "20200101000000Z", "-enddate", "20200102000000Z" if expired else "20400101000000Z")
        run("x509", "-in", f"{name}.cert.pem", "-outform", "DER", "-out", f"{name}.cert.der")

    for ca in ("ca1", "ca2"):
        key(ca)
        run("req", "-new", "-x509", "-sha256", "-days", "7305", "-key", f"{ca}.key.pem", "-out", f"{ca}.cert.pem", "-subj", f"/CN=KL11-public-test-{ca}", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
        run("x509", "-in", f"{ca}.cert.pem", "-outform", "DER", "-out", f"{ca}.cert.der")
        authority(ca)
    certificate("server1", "ca1", "serverAuth", dns="localhost")
    certificate("server2", "ca2", "serverAuth", dns="localhost")
    certificate("server-wrong-name", "ca1", "serverAuth", dns="wrong.invalid")
    certificate("server-expired", "ca1", "serverAuth", expired=True, dns="localhost")
    certificate("client1", "ca1", "clientAuth")
    certificate("client2", "ca2", "clientAuth")
    certificate("client-expired", "ca1", "clientAuth", expired=True)
    certificate("client-wrong-purpose", "ca1", "serverAuth")
    certificate("intermediate", "ca1", "", is_ca=True)
    authority("intermediate")
    certificate("client-chain", "intermediate", "clientAuth")
    for source in scratch.iterdir():
        if source.name.endswith((".cert.der", ".cert.pem", ".key.der", ".key.pem")):
            if source.name.startswith(("ca1.key", "ca2.key", "intermediate.key")):
                continue
            shutil.copyfile(source, destination / source.name)

metadata = {
    "warning": "PUBLIC TEST-ONLY private keys; never use in a deployed identity",
    "generator": subprocess.check_output(["openssl", "version"], text=True).strip(),
    "leaf_validity": "2020-01-01 through 2040-01-01 UTC; expired leaves end 2020-01-02",
    "commands": commands,
    "sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(destination.iterdir()) if p.is_file()},
}
(Path(__file__).resolve().parent / "fixtures.json").write_text(json.dumps(metadata, indent=2) + "\n")
print(f"Generated {len(metadata['sha256'])} bounded fixture files with {metadata['generator']}")
