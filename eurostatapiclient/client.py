import os
import requests
from .utils.property_decorators import property_is_string
from .models.dataset import Dataset


class EurostatAPIClient(object):
    """Represent an API client that handle Dataset Request
    and return dataset object.

    Parameters
    ----------
    version : String
        version of the API
    response_type : String
        type of file returned (should be 'json')
    language : String
        2 letter code for the language of the label and metadata.
        Should be one of these these : fr, en, de

    Attributes
    ----------
    BASE_URL : String
        Root Url of the Eurostat API
    version
    response_type
    language

    """
    BASE_URL = 'https://ec.europa.eu/eurostat/wdds/rest/data'
    session = requests.Session()

    def __init__(self, version="v2.1", response_type="json", language="en"):
        self.version = version
        self.response_type = response_type
        self.language = language
        self.url = ""
        self.params = []
        self.Dataset = None

    def set_proxy(self, proxy_dict):
        """
            set proxy for connection (in requests'format) : 
            ex {'http':'http://my.proxy:8080', 'https':'http://my.proxy:8080'}
        """
        self.session.proxies.update(proxy_dict)

    @property
    def version(self):
        return self._version

    @version.setter
    @property_is_string
    def version(self, value):
        self._version = value

    @property
    def response_type(self):
        return self._response_type

    @response_type.setter
    @property_is_string
    def response_type(self, value):
        self._response_type = value

    @property
    def language(self):
        return self._language

    @language.setter
    @property_is_string
    def language(self, value):
        self._language = value

    @property
    def api_url(self):
        return '{0}/{1}/{2}/{3}'.format(self.BASE_URL,
                                        self.version,
                                        self.response_type,
                                        self.language)

    @property
    def df(self):
        if self.Dataset is None:
            raise ValueError("Can't load DataFrame: Dataset is None!")
        return self.Dataset.to_dataframe()

    def load_dataset(self, id, params={}, verify=True):
        if verify == False:
            from requests.packages.urllib3.exceptions import InsecureRequestWarning
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        else:
            requests.packages.urllib3.warnings.resetwarnings()

        self.url = '{0}/{1}'.format(self.api_url, id)
        self.params = params

        response = self.session.get(self.url, params=self.params, verify=verify)
        response.raise_for_status()
        self.Dataset = Dataset.create_from_json(response.json())
        return self

    def read_json(self, json_or_fname, encoding="utf-8"):

        if isinstance(json_or_fname, (dict, list)):
            pass
        else:
            import json
            if os.path.isfile(json_or_fname):
                with open(json_or_fname, 'r', encoding=encoding) as f:
                    json_or_fname = json.load(f)
            else:
                json_or_fname = json.loads(json_or_fname)

        self.Dataset = Dataset.create_from_json(json_or_fname)
        return self

    def save_json(self, fname, encoding="utf-8", ensure_ascii=False, indent=4):
        if self.Dataset is None:
            raise ValueError("Can't save 'json': Dataset is None!")

        import json
        with open(fname, 'w', encoding=encoding) as f:
            json.dump(self.Dataset.json, f, ensure_ascii=ensure_ascii, indent=indent)

        return True

    def to_dataframe(self, **kwargs):
        if self.Dataset is None:
            raise ValueError("Can't load DataFrame: Dataset is None!")
        return self.Dataset.to_dataframe(**kwargs)
