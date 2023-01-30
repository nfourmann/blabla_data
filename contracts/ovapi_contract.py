contract = {
    "producer": {
        "name": "OVAPI",
        "description": "https://github.com/koch-t/KV78Turbo-OVAPI/wiki",
        "api_version": "http://v0.ovapi.nl/",
        "url": "http://v0.ovapi.nl/",
    },
    "data_exchanged": {
        "line": {
            "target": {
                "endpoint": "line/",
                "description": "https://github.com/koch-t/KV78Turbo-OVAPI/wiki",
                "output_schema": [
                    "DataOwnerCode",
                    "DestinationCode",
                    "DestinationName50",
                    "LineDirection",
                    "LineName",
                    "LinePlanningNumber",
                    "LinePublicNumber",
                    "LineWheelchairAccessible",
                    "TransportType",
                ],
            },
            "entity": {
                "filename": "domain/entities/public_transport/netherlands/lines.py"
            },
            "workflow": {"filename": "dags/ingest_netherlands_public_transport.py"},
            "destination": {
                "db_name": "raw_db",
                "schema_name": "raw_ovapi",
                "table_name": "raw_lines",
                "partition_by": "day",
                "output_schema": [
                    "data_owner_code",
                    "destination_code",
                    "destination_name_50",
                    "line_direction",
                    "line_name",
                    "line_planningNumber",
                    "line_public_number",
                    "line_wheel_chair_accessible",
                    "transport_type",
                    "line_code",
                ],
            },
        }
    },
}

producer_config = contract.get("producer")
line_config = contract.get("data_exchanged").get("line")

line_target = line_config.get("target")
line_destination = line_config.get("destination")
