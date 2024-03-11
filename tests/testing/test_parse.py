from ksqldb_client.testing import parse


def test_extract_columns_from_ksql_schema():
    schema = "`BOOKING_LOCATOR` STRING, `PNR_CREATE_DATE` TIMESTAMP, `TICKET_NUM` INTEGER, `COUPONS_INTERESTING_FIELDS` STRUCT<`COUPON_NUM` INTEGER, `BOARDPOINT_STN_CODE` STRING, `OFFPOINT_STN_CODE` STRING, `TICKET_SALE_DATE_TIME` TIMESTAMP>, `NEW` STRUCT<`COUPON_NUM` INTEGER>"  # noqa: E501

    result = parse.extract_columns_from_ksql_schema(schema)

    expected_result = {
        "BOOKING_LOCATOR": "STRING",
        "PNR_CREATE_DATE": "TIMESTAMP",
        "TICKET_NUM": "INTEGER",
        "COUPONS_INTERESTING_FIELDS": "STRUCT",
        "NEW": "STRUCT",
    }

    assert result == expected_result


def test_dictionary_keys_to_lowercase():
    dict_ = {
        "Key1": "Value1",
        "Key2": {
            "SubKey1": "SubValue1",
            "SubKey2": {
                "SubSubKey1": "SubSubValue1",
            },
        },
        "Key3": (
            {
                "ListKey1": "ListValue1",
            },
            "ListValue2",
        ),
    }

    result = parse.dictionary_keys_to_lowercase(dict_)

    expected_result = {
        "key1": "Value1",
        "key2": {
            "subkey1": "SubValue1",
            "subkey2": {
                "subsubkey1": "SubSubValue1",
            },
        },
        "key3": (
            {
                "listkey1": "ListValue1",
            },
            "ListValue2",
        ),
    }

    assert result == expected_result
