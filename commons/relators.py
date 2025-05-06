import json
import os
import re

RELATORS_JSON_PATH = os.path.join(os.path.dirname(__file__), "relators.json")


def load_relator_mapping():
    with open(RELATORS_JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
        return {
            # Extract last segment of URI as code (e.g., 'fmo')
            entry["@id"].split("/")[-1]: entry['http://www.loc.gov/mads/rdf/v1#authoritativeLabel'][0]["@value"]
            for entry in data
            if "@id" in entry and 'http://www.loc.gov/mads/rdf/v1#authoritativeLabel' in entry
        }


RELATOR_URI_TO_LABEL = load_relator_mapping()


def extract_relator_code(uri: str) -> str:
    """Extracts the short relator code from a full URI or .html URL."""
    # Examples:
    # 'http://id.loc.gov/vocabulary/relators/fmo' => 'fmo'
    # 'https://id.loc.gov/vocabulary/relators/pbd.html' => 'pbd'
    return re.sub(r"\.html$", "", uri.strip().split("/")[-1])

