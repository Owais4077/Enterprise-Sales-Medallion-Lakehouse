"""Download the Delta Lake JARs into PySpark's jar folder at image build time.

Doing this at build time (not at first Spark start) means no internet is needed at runtime,
builds are reproducible, and every JAR is verified against the SHA-1 published by Maven Central.
"""

import hashlib
import os
import urllib.request

import pyspark

DELTA_VERSION = "3.2.1"
BASE = "https://repo1.maven.org/maven2/io/delta"
JARS = [
    f"delta-spark_2.12/{DELTA_VERSION}/delta-spark_2.12-{DELTA_VERSION}.jar",
    f"delta-storage/{DELTA_VERSION}/delta-storage-{DELTA_VERSION}.jar",
]

target = os.path.join(os.path.dirname(pyspark.__file__), "jars")
for jar in JARS:
    url = f"{BASE}/{jar}"
    data = urllib.request.urlopen(url, timeout=120).read()  # noqa: S310  (fixed https URL)
    published = urllib.request.urlopen(url + ".sha1", timeout=60).read()  # noqa: S310
    expected = published.decode().strip().split()[0]
    # SHA-1 is only an integrity check against the checksum Maven Central publishes.
    actual = hashlib.sha1(data, usedforsecurity=False).hexdigest()
    if actual != expected:
        raise SystemExit(f"Checksum mismatch for {jar}: {actual} != {expected}")
    with open(os.path.join(target, os.path.basename(jar)), "wb") as handle:
        handle.write(data)
    print(f"installed {os.path.basename(jar)} ({len(data):,} bytes, sha1 verified)")
