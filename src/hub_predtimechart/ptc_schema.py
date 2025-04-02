ptc_config_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://github.com/reichlab/hub-dashboard-predtimechart/src/hub_predtimechart/schema.json",
    "title": "predtimechart options",
    "description": "hub predtimechart configuration object schema",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "rounds_idx": {
            "description": "0-based index of the `rounds` entry to use for visualization",
            "type": "integer",
            "minimum": 0
        },
        "reference_date_col_name": {
            "description": "name of the column that represents the `reference_date`",
            "type": "string",
            "minLength": 1
        },
        "target_date_col_name": {
            "description": "name of the column that represents the `target_date`",
            "type": "string",
            "minLength": 1
        },
        "horizon_col_name": {
            "description": "name of the column that represents the `horizon`",
            "type": "string",
            "minLength": 1
        },
        "initial_checked_models": {
            "description": "models id(s) to use for the initial plot",
            "type": "array",
            "uniqueItems": True,
            "items": {
                "type": "string"
            }
        },
        "disclaimer": {
            "description": "text providing any important information users should know",
            "type": "string",
            "minLength": 1
        },
        "task_id_text": {
            "description": "optional mapping of predtimechart task id values to text. keys are task_ids and values are an object whose keys are ptc task_id `value`s and values are ptc task_id `text`s",
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": {
                    "type": "string"
                }
            }
        },
        "target_data_file_name": {
            "description": "optional name of the target data file located in the hub's target-data dir",
            "type": "string",
            "minLength": 1
        },
    },
    "required": [
        "rounds_idx",
        "reference_date_col_name",
        "target_date_col_name",
        "horizon_col_name",
        "initial_checked_models"
    ]
}
