import pandas as pd
from contracts.ovapi_contract import line_target, line_destination
from infrastructure.connectors.ovapi.client import OVAPI


class LinesRecords:
    def __init__(self, ovapi_client):

        self.target_schema = line_target.get("output_schema")
        self.destination_schema = line_destination.get("output_schema")

        self.raw_data = ovapi_client.extract("lines")
        self.data = self.__data_preparation(
            raw_data=ovapi_client.extract(data_point="lines")
        )

    def __data_preparation(
        self,
        raw_data,
    ):

        df = pd.DataFrame.from_dict(raw_data, orient="index")
        df = df.rename_axis("LineCode").reset_index()

        self.target_schema.append("LineCode")

        mapping = {
            t_col: d_col
            for (t_col, d_col) in zip(self.target_schema, self.destination_schema)
        }
        df = df.rename(columns=mapping)
        return df

    def csv_dumps(self):
        return self.data.to_csv(index=False)
