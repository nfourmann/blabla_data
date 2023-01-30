import requests

from contracts.ovapi_contract import producer_config, line_config


class OVAPI:
    """
    usage:
        client = OVAPI()
        lines_records = client.extract(lines)
    test cover:
        - catch all https errors and raise error.
        - assert output schema is correct.
    """

    BASE_URL = producer_config.get("url")
    LINE_ENDPOINT = line_config.get("target").get("endpoint")

    def __init__(self):
        self.session = requests.Session()
        self.session.hooks["response"] = [
            lambda response, *args, **kwargs: response.raise_for_status()
        ]

    def __get_lines(self):
        request_url = self.BASE_URL + self.LINE_ENDPOINT
        r = self.session.get(request_url)
        return r.json()

    def __lines_schema_validation(self, raw_lines):
        ref_schema = set(line_config.get("target").get("output_schema"))
        raw_schema = set()

        for line_item in raw_lines.values():
            for key in line_item.keys():
                if key not in raw_schema:
                    raw_schema.add(key)

        if raw_schema.difference(ref_schema):
            return False
        else:
            return True

    def extract(self, data_point):
        if data_point == "lines":
            data = self.__get_lines()
            if not self.__lines_schema_validation(data):
                raise ValueError(f"The line schema has changed from the source!")
        else:
            raise ValueError(f"This {data_point} data point is not implemented!")
        return data
