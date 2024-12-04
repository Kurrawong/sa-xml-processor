from enum import Enum
from pathlib import Path
from typing import Union, Optional

import httpx
from lxml import etree
from rdflib import Namespace, URIRef
from hashlib import sha1

NAMESPACES = {
    # general
    "xlink": "http://www.w3.org/1999/xlink",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",

    # 19139
    "csw": "http://www.opengis.net/cat/csw/2.0.2",
    "ogc": "http://www.opengis.net/ogc",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "ows": "http://www.opengis.net/ows",
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gmx": "http://www.isotc211.org/2005/gmx",
    "srv": "http://www.isotc211.org/2005/srv",
    "gmi": "http://www.isotc211.org/2005/gmi",
    "gts": "http://www.isotc211.org/2005/gts",
    "pubSub": "http://www.opengis.net/pubsub/1.0",
    "ows11": "http://www.opengis.net/ows/1.1",
    "gml32": "http://www.opengis.net/gml/3.2",
    "xacml": "urn:oasis:names:tc:xacml:3.0:core:schema:wd-17",
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "xs": "http://www.w3.org/2001/XMLSchema",
    "mdb": "http://standards.iso.org/iso/19115/-3/mdb/1.0",

    # 19115-1
    "cat": "http://standards.iso.org/iso/19115/-3/cat/1.0",
    "gfc": "http://standards.iso.org/iso/19110/gfc/1.1",
    "cit": "http://standards.iso.org/iso/19115/-3/cit/1.0",
    "gcx": "http://standards.iso.org/iso/19115/-3/gcx/1.0",
    "gex": "http://standards.iso.org/iso/19115/-3/gex/1.0",
    "lan": "http://standards.iso.org/iso/19115/-3/lan/1.0",
    # "srv": "http://standards.iso.org/iso/19115/-3/srv/2.0",
    "mas": "http://standards.iso.org/iso/19115/-3/mas/1.0",
    "mcc": "http://standards.iso.org/iso/19115/-3/mcc/1.0",
    "mco": "http://standards.iso.org/iso/19115/-3/mco/1.0",
    "mda": "http://standards.iso.org/iso/19115/-3/mda/1.0",
    "mds": "http://standards.iso.org/iso/19115/-3/mds/1.0",
    "mdt": "http://standards.iso.org/iso/19115/-3/mdt/1.0",
    "mex": "http://standards.iso.org/iso/19115/-3/mex/1.0",
    "mmi": "http://standards.iso.org/iso/19115/-3/mmi/1.0",
    "mpc": "http://standards.iso.org/iso/19115/-3/mpc/1.0",
    "mrc": "http://standards.iso.org/iso/19115/-3/mrc/1.0",
    "mrd": "http://standards.iso.org/iso/19115/-3/mrd/1.0",
    "mri": "http://standards.iso.org/iso/19115/-3/mri/1.0",
    "mrl": "http://standards.iso.org/iso/19115/-3/mrl/1.0",
    "mrs": "http://standards.iso.org/iso/19115/-3/mrs/1.0",
    "msr": "http://standards.iso.org/iso/19115/-3/msr/1.0",
    "mdq": "http://standards.iso.org/iso/19157/-2/mdq/1.0",
    "mac": "http://standards.iso.org/iso/19115/-3/mac/1.0",
}
NAMESPACES_19139 = {
    "gco": "http://www.isotc211.org/2005/gco",
    "gml": "http://www.opengis.net/gml",
}
NAMESPACES_19115_1 = {
    "gco": "http://standards.iso.org/iso/19115/-3/gco/1.0",
    "gml": "http://www.opengis.net/gml/3.2",
}
EX = Namespace("http://example.com/")
KW = Namespace("https://w3id.org/kw/")


class Profile(Enum):
    SEADATANET = "SeaDataNet"
    ISO19115 = "ISO19115"
    ISO19139 = "ISO19139"
    UNKNOWN = "Unknown"


def str_tidy(s):
    return " ".join(s.split())


def get_metadata_profile(path_to_file_or_etree: Union[Path, etree]):
    et = path_to_file_or_etree if not isinstance(path_to_file_or_etree, Path) else etree.parse(path_to_file_or_etree)

    r = et.xpath(
        "//gmi:MI_Metadata/gmd:metadataExtensionInfo/@xlink:href",
        namespaces={**NAMESPACES, **NAMESPACES_19139},
    )

    if len(r) > 0:
        return Profile.SEADATANET

    r = et.xpath(
        "//gmi:MI_Metadata",
        namespaces={**NAMESPACES, **NAMESPACES_19139},
    )

    if len(r) > 0:
        return Profile.ISO19139

    r = et.xpath(
        "//mdb:MD_Metadata",
        namespaces={**NAMESPACES, **NAMESPACES_19115_1},
    )

    if len(r) > 0:
        return Profile.ISO19115

    return Profile.UNKNOWN


def get_id(path_to_file_or_etree: Union[Path, etree], profile: Optional[Profile] = None):
    et = path_to_file_or_etree if not isinstance(path_to_file_or_etree, Path) else etree.parse(path_to_file_or_etree)

    if profile is None:
        profile = get_metadata_profile(et)

    if profile == Profile.ISO19115:
        r = et.xpath(
            "//mdb:metadataIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            namespaces={**NAMESPACES, **NAMESPACES_19115_1},
        )
    else:
        r = et.xpath(
            "//gmd:fileIdentifier/gco:CharacterString/text()",
            namespaces={**NAMESPACES, **NAMESPACES_19139},
        )

    if len(r) > 0:
        return r[0]


def make_record_iri(id: str) -> URIRef:
    return "http://example.com/record/" + id


def send_query_to_db(query):
    r = httpx.get(
        "http://localhost:3030/ds",
        headers={"Accept": "application/sparql-results+json"},
        params={"query": query},
        timeout=20
    )
    if r.status_code == 400:
        print(r.text)
        print(query)
    try:
        if r.json().get("boolean"):
            return r.json()["boolean"]
        else:
            return r.json()["results"]["bindings"]
    except Exception as e:
        print(e)
        print(query)
        exit()
        return {}


def upload_file_to_db(ttl_file: Path, graph_iri: str):
    r = httpx.post(
        "http://localhost:3030/ds",
        params={"graph": graph_iri},
        headers={"Content-Type": "text/turtle"},
        data=ttl_file.read_bytes()
    )

    return r.status_code, r.text


def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text